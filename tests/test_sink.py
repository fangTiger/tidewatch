from datetime import timedelta

import pytest
from sqlalchemy import select, text

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


async def test_commit_failure_keeps_cursor(sink, engine, clock):
    broken = _flash(clock, "jin10:x", "标题")
    broken.title = None  # 违反 NOT NULL，整批事务失败
    with pytest.raises(Exception):
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
