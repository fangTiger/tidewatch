import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.rules.engine import build_rule_engine
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.repo import load_cursor
from tidewatch.store.tables import events, push_log

NFP = "美国9月非农就业人口增加8.2万人"


def _flash(clock, key: str, title: str, *, important: int = 1, minutes_ago: int = 1) -> Event:
    return Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=clock.now - timedelta(minutes=minutes_ago),
        title=title,
        dedupe_key=key,
        raw={"important": important},
    )


@pytest.fixture
async def sink(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    yield event_sink
    await event_sink.stop()


async def test_ingest_grades_clusters_and_plans(sink, engine, clock):
    received = []
    sink.on_ingest(received.append)
    batch = FetchResult([_flash(clock, "jin10:1", NFP), _flash(clock, "jin10:2", "港股低开高走，恒指翻红")], {"last_id": "2"})
    ids = await sink.ingest(batch, collector="jin10")
    async with engine.connect() as conn:
        rows = {r.dedupe_key: r for r in (await conn.execute(select(events))).all()}
        pushes = (await conn.execute(select(push_log))).all()
        cursor = await load_cursor(conn, "jin10")
    assert len(ids) == 2
    assert rows["jin10:1"].final_level == "S" and rows["jin10:1"].ai_status == "skipped"
    assert rows["jin10:2"].final_level == "B" and rows["jin10:2"].cluster_id is not None
    assert rows["jin10:1"].cluster_id != rows["jin10:2"].cluster_id
    assert len(pushes) == 1 and pushes[0].level == "S" and pushes[0].status == "pending"
    assert cursor == {"last_id": "2"}
    assert len(received) == 1 and received[0].event_ids == ids and received[0].plans[0].level == "S"


async def test_assets_merged_with_watchlist(sink, engine, clock):
    event = _flash(clock, "jin10:3", "比特币突破12万美元", important=0)
    event.assets = ["XYZ"]
    await sink.ingest(FetchResult([event], None), collector="jin10")
    async with engine.connect() as conn:
        row = (await conn.execute(select(events.c.assets, events.c.final_level))).one()
    assert row.assets == ["XYZ", "BTC"] and row.final_level == "B"


async def test_duplicate_ingest_is_ignored(sink, engine, clock):
    assert len(await sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], {"last_id": "1"}), collector="jin10")) == 1
    assert await sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], {"last_id": "5"}), collector="jin10") == []
    async with engine.connect() as conn:
        assert len((await conn.execute(select(push_log))).all()) == 1
        assert await load_cursor(conn, "jin10") == {"last_id": "5"}


async def test_rule_error_degrades_single_event(engine, settings, clock):
    real = build_rule_engine(settings)

    class Boom:
        async def evaluate(self, event, ctx):
            if event.dedupe_key == "bad":
                raise RuntimeError("规则缺陷")
            return await real.evaluate(event, ctx)

    event_sink = create_event_sink(engine, settings, clock=clock, rules=Boom())
    await event_sink.start()
    try:
        batch = FetchResult([_flash(clock, "bad", NFP), _flash(clock, "good", "美国宣布对中国加征关税")], {"last_id": "9"})
        await event_sink.ingest(batch, collector="jin10")
    finally:
        await event_sink.stop()
    async with engine.connect() as conn:
        rows = {r.dedupe_key: r for r in (await conn.execute(select(events))).all()}
        pushes = (await conn.execute(select(push_log))).all()
        cursor = await load_cursor(conn, "jin10")
    assert rows["bad"].final_level == "C" and rows["bad"].rule_hits == ["error"] and rows["bad"].cluster_id is not None
    assert rows["good"].final_level == "S" and len(pushes) == 1
    assert cursor == {"last_id": "9"}


async def test_db_error_in_rule_rolls_back_to_savepoint(engine, settings, clock):
    """规则里发生数据库错误（会使事务进入中止状态）时，必须回滚到保存点，同批其余事件照常提交"""
    real = build_rule_engine(settings)

    class DbBoom:
        async def evaluate(self, event, ctx):
            if event.dedupe_key == "bad":
                await ctx.conn.execute(text("SELECT 1/0"))
            return await real.evaluate(event, ctx)

    event_sink = create_event_sink(engine, settings, clock=clock, rules=DbBoom())
    await event_sink.start()
    try:
        batch = FetchResult([_flash(clock, "bad", NFP), _flash(clock, "good", "美国宣布对中国加征关税")], {"last_id": "7"})
        await event_sink.ingest(batch, collector="jin10")
    finally:
        await event_sink.stop()
    async with engine.connect() as conn:
        rows = {r.dedupe_key: r for r in (await conn.execute(select(events))).all()}
        assert await load_cursor(conn, "jin10") == {"last_id": "7"}
    assert rows["bad"].final_level == "C" and rows["good"].final_level == "S"
    assert rows["bad"].rule_hits == ["error"] and rows["bad"].cluster_id is not None


async def test_insert_failure_rolls_back_batch_and_keeps_cursor(sink, engine, clock):
    broken = _flash(clock, "jin10:x", "标题")
    broken.title = None  # 违反 NOT NULL，整批事务失败
    with pytest.raises(DBAPIError):
        await sink.ingest(FetchResult([_flash(clock, "jin10:ok", "港股低开高走"), broken], {"last_id": "x"}), collector="jin10")
    async with engine.connect() as conn:
        assert (await conn.execute(select(events))).first() is None
        assert await load_cursor(conn, "jin10") is None
    # 工作协程仍然可用
    assert len(await sink.ingest(FetchResult([_flash(clock, "jin10:ok", "港股低开高走")], {"last_id": "y"}), collector="jin10")) == 1


async def test_ai_enabled_marks_pending(engine, settings, clock):
    settings.ai.enabled = True
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    try:
        batch = FetchResult([_flash(clock, "b", "港股低开高走，恒指翻红"), _flash(clock, "c", "印度银行提供新服务", important=0)], None)
        await event_sink.ingest(batch, collector="jin10")
    finally:
        await event_sink.stop()
    async with engine.connect() as conn:
        rows = {r.dedupe_key: r for r in (await conn.execute(select(events))).all()}
    assert rows["b"].ai_status == "pending" and rows["c"].ai_status == "skipped"


async def test_exchange_assets_from_title_only(sink, engine, clock):
    event = Event(source="okx_ann", category="exchange", event_type="delisting", occurred_at=clock.now,
                  title="OKX to delist ABC", body="相关交易对 ABC/BTC、ABC/ETH 将下架", dedupe_key="okx:1",
                  assets=["ABC"], raw={"exchange": "okx"})
    await sink.ingest(FetchResult([event], None), collector="okx_ann")
    async with engine.connect() as conn:
        row = (await conn.execute(select(events.c.assets, events.c.final_level))).one()
    assert row.assets == ["ABC"] and row.final_level == "B"


async def test_not_started_raises(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    with pytest.raises(RuntimeError):
        await event_sink.ingest(FetchResult([], None), collector="jin10")
    await event_sink.start()
    await event_sink.stop()
    with pytest.raises(RuntimeError):  # 停止后再提交也立即报错，不会挂起
        await event_sink.ingest(FetchResult([], None), collector="jin10")


async def test_duplicate_ingest_does_not_notify(sink, clock):
    received = []
    sink.on_ingest(received.append)
    await sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], {"last_id": "1"}), collector="jin10")
    assert len(received) == 1
    assert await sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], {"last_id": "2"}), collector="jin10") == []
    assert len(received) == 1  # 没有新事件，不通知


async def test_listener_exception_is_isolated(sink, clock):
    """一个监听器抛异常，既不影响其他监听器，也不影响 ingest 的返回"""
    received = []

    def broken(_outcome):
        raise RuntimeError("监听器缺陷")

    sink.on_ingest(broken)
    sink.on_ingest(received.append)
    ids = await sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], None), collector="jin10")
    assert len(ids) == 1 and len(received) == 1 and received[0].event_ids == ids


class _TrackedEngine:
    """包装引擎：记录 begin() 的事务是否已经提交（__aexit__ 正常完成）"""

    def __init__(self, engine) -> None:
        self._engine = engine
        self.committed = False

    def begin(self):
        tracked = self

        class _Begin:
            def __init__(self) -> None:
                self._inner = tracked._engine.begin()

            async def __aenter__(self):
                tracked.committed = False
                return await self._inner.__aenter__()

            async def __aexit__(self, exc_type, exc, tb):
                result = await self._inner.__aexit__(exc_type, exc, tb)
                if exc_type is None:
                    tracked.committed = True
                return result

        return _Begin()


async def test_listeners_notified_only_after_commit(engine, settings, clock):
    tracked = _TrackedEngine(engine)
    event_sink = create_event_sink(tracked, settings, clock=clock)
    await event_sink.start()
    observed: dict[str, bool] = {}
    # 监听器中的断言异常会被吞掉，因此只记录当时的提交状态，事后再断言
    event_sink.on_ingest(lambda outcome: observed.setdefault("ingest", tracked.committed))
    event_sink.on_push_result(lambda outcome: observed.setdefault("push", tracked.committed))
    try:
        await event_sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], None), collector="jin10")
        async with engine.connect() as conn:
            push_id = (await conn.execute(select(push_log.c.id))).scalar_one()
        await event_sink.push_result(push_id, "ok")
    finally:
        await event_sink.stop()
    assert observed == {"ingest": True, "push": True}


class _BlockingRules:
    """第一次 evaluate 时卡住，直到 release 被置位；之后委托给真实规则"""

    def __init__(self, real) -> None:
        self._real = real
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def evaluate(self, event, ctx):
        self.entered.set()
        await self.release.wait()
        return await self._real.evaluate(event, ctx)


async def _wait_queued(event_sink, count: int) -> None:
    while event_sink._queue.qsize() < count:
        await asyncio.sleep(0)


async def test_worker_cancel_fails_running_and_queued_commands(engine, settings, clock):
    """工作协程被取消时，正在执行与排队中的命令都应立即失败，而不是让调用方永久等待"""
    blocking = _BlockingRules(build_rule_engine(settings))
    event_sink = create_event_sink(engine, settings, clock=clock, rules=blocking)
    await event_sink.start()
    first = asyncio.create_task(event_sink.ingest(FetchResult([_flash(clock, "k1", NFP)], None), collector="jin10"))
    await asyncio.wait_for(blocking.entered.wait(), 5)  # 第一条命令已在执行并卡住
    second = asyncio.create_task(event_sink.ingest(FetchResult([_flash(clock, "k2", NFP)], None), collector="jin10"))
    await asyncio.wait_for(_wait_queued(event_sink, 1), 5)
    event_sink._worker.cancel()
    for task in (first, second):
        with pytest.raises(RuntimeError, match="已停止"):
            await asyncio.wait_for(task, 3)
    await event_sink.stop()  # 工作协程已被取消，stop 仍应正常返回


async def test_submit_fails_fast_after_worker_exit(engine, settings, clock):
    """工作协程已退出（被取消）后再提交命令：立即报错而不是入队后永久挂起；stop 之后可重新启动"""
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    worker = event_sink._worker
    worker.cancel()
    await asyncio.wait({worker})
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(event_sink.ingest(FetchResult([], None), collector="jin10"), 3)
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(event_sink.push_result(1, "ok"), 3)
    await event_sink.stop()
    assert event_sink._worker is None
    await event_sink.start()
    try:
        assert len(await event_sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], None), collector="jin10")) == 1
    finally:
        await event_sink.stop()


async def test_cancelling_stop_does_not_cancel_worker(engine, settings, clock):
    """stop() 被外层取消时不能连带取消工作协程：已在执行的命令仍应正常完成"""
    blocking = _BlockingRules(build_rule_engine(settings))
    event_sink = create_event_sink(engine, settings, clock=clock, rules=blocking)
    await event_sink.start()
    worker = event_sink._worker
    ingest = asyncio.create_task(event_sink.ingest(FetchResult([_flash(clock, "k1", NFP)], None), collector="jin10"))
    await asyncio.wait_for(blocking.entered.wait(), 5)
    stopping = asyncio.create_task(event_sink.stop())
    await asyncio.sleep(0.05)  # 让 stop() 进入等待
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping
    assert not worker.done()
    blocking.release.set()
    assert len(await asyncio.wait_for(ingest, 5)) == 1
    await event_sink.stop()  # 再次 stop 等待工作协程自然结束
    assert event_sink._worker is None
