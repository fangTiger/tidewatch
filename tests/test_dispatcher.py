import asyncio
import logging
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import insert, select, update

from fakes import FakeNotifier
from tidewatch.notify.base import EventCard, SendResult
from tidewatch.notify.dispatcher import Dispatcher
from tidewatch.notify.dryrun import DryRunNotifier
from tidewatch.pipeline.decision import PushPlan
from tidewatch.pipeline.sink import IngestOutcome, create_event_sink
from tidewatch.store.tables import event_clusters, events, push_log


class FakeMonotonic:
    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value


class _BlockingANotifier(FakeNotifier):
    """阻塞 A 级发送，便于验证 S 级使用独立发送协程"""

    def __init__(self) -> None:
        super().__init__()
        self.a_started = asyncio.Event()
        self.release_a = asyncio.Event()
        self.s_sent = asyncio.Event()

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        self.events.append((card, mention_all))
        if card.level == "A":
            self.a_started.set()
            await self.release_a.wait()
        else:
            self.s_sent.set()
        return self._next()


class _CancelAfterGetQueue(asyncio.Queue):
    """取出元素后取消当前消费者，稳定复现 get 与 _handle 之间的取消窗口"""

    async def get(self):
        item = await super().get()
        asyncio.current_task().cancel()
        return item


@pytest.fixture
async def sink(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    yield event_sink
    await event_sink.stop()


async def _seed(engine, clock, level: str, n: int = 1, *, status: str = "pending", retry_minutes: int = 30,
                kind: str = "event") -> list[int]:
    """直接写入 n 个簇、事件与推送记录，返回 push_id 列表"""
    ids = []
    async with engine.begin() as conn:
        for i in range(n):
            cid = (await conn.execute(insert(event_clusters).values(
                category="news", first_seen_at=clock.now, last_seen_at=clock.now,
                max_level=level, committed_level=level).returning(event_clusters.c.id))).scalar_one()
            eid = (await conn.execute(insert(events).values(
                source="jin10", category="news", event_type="flash", occurred_at=clock.now, collected_at=clock.now,
                title=f"{level} 级测试 {i}", dedupe_key=f"t:{uuid.uuid4().hex}", final_level=level,
                cluster_id=cid).returning(events.c.id))).scalar_one()
            pid = (await conn.execute(insert(push_log).values(
                cluster_id=cid, event_id=eid, level=level, kind=kind, status=status,
                retry_until=clock.now + timedelta(minutes=retry_minutes), created_at=clock.now,
            ).returning(push_log.c.id))).scalar_one()
            ids.append(pid)
    return ids


def _dispatcher(engine, sink, settings, clock, notifier, *, monotonic=None, sleeps=None, sleep=None) -> Dispatcher:
    async def fake_sleep(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    return Dispatcher(engine, sink, notifier, settings.dispatch, clock=clock, sleep=sleep or fake_sleep,
                      monotonic=monotonic or FakeMonotonic())


async def _idle_sleep(seconds: float) -> None:
    """真正让出事件循环的睡眠：后台定时任务在测试期间不会被触发，也不会空转占满事件循环"""
    await asyncio.sleep(3600)


async def _wait_status(engine, ids: list[int], expected: str, timeout: float = 5.0) -> None:
    """等待所有记录进入指定状态；超时抛 TimeoutError，避免后台任务出问题时测试挂起"""

    async def poll() -> None:
        while any(v[0] != expected for k, v in (await _statuses(engine)).items() if k in ids):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


async def _statuses(engine) -> dict[int, tuple[str, str | None]]:
    async with engine.connect() as conn:
        rows = (await conn.execute(select(push_log.c.id, push_log.c.status, push_log.c.batch_id))).all()
    return {r.id: (r.status, r.batch_id) for r in rows}


async def test_s_sent_immediately_with_mention(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert notifier.events[0][1] is True and notifier.events[0][0].title == "S 级测试 0"
    assert (await _statuses(engine))[pid][0] == "ok"


async def test_s_send_is_not_blocked_by_a_send(engine, sink, settings, clock):
    notifier = _BlockingANotifier()
    [a_id] = await _seed(engine, clock, "A")
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=_idle_sleep)
    await dispatcher.start()
    try:
        await asyncio.wait_for(notifier.a_started.wait(), 1)
        [s_id] = await _seed(engine, clock, "S")
        dispatcher.enqueue([s_id])
        await asyncio.wait_for(notifier.s_sent.wait(), 1)
        assert [card.push_id for card, _ in notifier.events] == [a_id, s_id]
    finally:
        notifier.release_a.set()
        await asyncio.wait_for(dispatcher.stop(), 5)


async def test_failed_s_retry_is_not_blocked_by_failed_a(engine, sink, settings, clock):
    notifier = _BlockingANotifier()
    await _seed(engine, clock, "A", status="failed")
    await _seed(engine, clock, "S", status="failed")
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=_idle_sleep)
    start_task = asyncio.create_task(dispatcher.start())
    try:
        await asyncio.wait_for(notifier.a_started.wait(), 1)
        await asyncio.wait_for(notifier.s_sent.wait(), 1)
    finally:
        notifier.release_a.set()
        await asyncio.wait_for(start_task, 5)
        await asyncio.wait_for(dispatcher.stop(), 5)


async def test_a_window_overflow_goes_to_digest(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 13)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert len(notifier.events) == 10 and dispatcher.digest_size == 3
    [s_id] = await _seed(engine, clock, "S")  # S 级不受窗口限制
    dispatcher.enqueue([s_id])
    await dispatcher.process_pending()
    assert len(notifier.events) == 11
    await dispatcher.flush_digest()
    assert len(notifier.digests) == 1 and len(notifier.digests[0]) == 3
    statuses = await _statuses(engine)
    batch_ids = {statuses[i][1] for i in ids[10:]}
    assert all(statuses[i][0] == "ok" for i in ids) and len(batch_ids) == 1 and None not in batch_ids


async def test_window_slides(engine, sink, settings, clock):
    notifier, mono = FakeNotifier(), FakeMonotonic()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, monotonic=mono)
    dispatcher.enqueue(await _seed(engine, clock, "A", 11))
    await dispatcher.process_pending()
    assert dispatcher.digest_size == 1
    mono.value += settings.dispatch.a_window_s + 1
    dispatcher.enqueue(await _seed(engine, clock, "A", 1))
    await dispatcher.process_pending()
    assert len(notifier.events) == 11


async def test_immediate_retry_then_success(engine, sink, settings, clock):
    sleeps: list[float] = []
    notifier = FakeNotifier([SendResult(False, retryable=True, detail="code=11232"), SendResult(True, detail="code=0")])
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleeps=sleeps)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert sleeps == [1.0] and (await _statuses(engine))[pid][0] == "ok"


async def test_retry_expires_during_capped_backoff(engine, sink, settings, clock):
    sleeps: list[float] = []

    async def advancing_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(seconds=seconds)

    notifier = FakeNotifier(
        [SendResult(False, retryable=True, detail="code=11232"), SendResult(True, detail="code=0")]
    )
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=advancing_sleep)
    [pid] = await _seed(engine, clock, "S")
    async with engine.begin() as conn:
        await conn.execute(
            update(push_log)
            .where(push_log.c.id == pid)
            .values(retry_until=clock.now + timedelta(seconds=0.25))
        )
    dispatcher.enqueue([pid])
    await asyncio.wait_for(dispatcher.process_pending(), 1)
    assert sleeps == [0.25]
    assert len(notifier.events) == 1
    assert (await _statuses(engine))[pid][0] == "expired"


async def test_non_retryable_marks_failed(engine, sink, settings, clock):
    notifier = FakeNotifier([SendResult(False, retryable=False, detail="code=19021")])
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and (await _statuses(engine))[pid][0] == "failed"


async def test_retry_failed_and_expire(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [alive] = await _seed(engine, clock, "A", status="failed", retry_minutes=30)
    [dead] = await _seed(engine, clock, "A", status="failed", retry_minutes=-1)
    [stuck] = await _seed(engine, clock, "S", status="pending", retry_minutes=-1)
    await dispatcher.retry_failed()
    statuses = await _statuses(engine)
    assert statuses[alive][0] == "ok" and statuses[dead][0] == "expired" and statuses[stuck][0] == "expired"
    assert len(notifier.events) == 1


async def test_retry_of_failed_digest_respects_window(engine, sink, settings, clock):
    notifier = FakeNotifier([SendResult(True)] * 10 + [SendResult(False, retryable=False, detail="code=19021")])
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 11)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    await dispatcher.flush_digest()  # 汇总发送失败 → 该条标为 failed
    assert (await _statuses(engine))[ids[10]][0] == "failed"
    await dispatcher.retry_failed()  # 窗口已满：回到汇总缓冲，而不是单独发送
    assert len(notifier.events) == 10 and dispatcher.digest_size == 1
    await dispatcher.flush_digest()
    assert (await _statuses(engine))[ids[10]][0] == "ok"


async def test_digest_rechecks_deadline_immediately_before_send(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 11)
    dispatcher.enqueue(ids)
    await asyncio.wait_for(dispatcher.process_pending(), 5)
    digest_id = ids[10]
    async with engine.begin() as conn:
        await conn.execute(
            update(push_log)
            .where(push_log.c.id == digest_id)
            .values(retry_until=clock.now + timedelta(seconds=1))
        )
    original_load = dispatcher._load

    async def load_then_expire(push_id: int):
        pending = await original_load(push_id)
        clock.advance(seconds=2)
        return pending

    dispatcher._load = load_then_expire
    await asyncio.wait_for(dispatcher.flush_digest(), 1)
    assert notifier.digests == []
    assert (await _statuses(engine))[digest_id][0] == "expired"


async def test_digest_rechecks_deadlines_after_expired_result_write(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 12)
    dispatcher.enqueue(ids)
    await asyncio.wait_for(dispatcher.process_pending(), 5)
    first, second = ids[10:]
    async with engine.begin() as conn:
        await conn.execute(
            update(push_log)
            .where(push_log.c.id == first)
            .values(retry_until=clock.now - timedelta(seconds=1))
        )
        await conn.execute(
            update(push_log)
            .where(push_log.c.id == second)
            .values(retry_until=clock.now + timedelta(seconds=1))
        )
    original_push_result = sink.push_result

    async def push_result_then_advance(push_id: int, status: str, **kwargs):
        result = await original_push_result(push_id, status, **kwargs)
        if status == "expired":
            clock.advance(seconds=2)
        return result

    sink.push_result = push_result_then_advance
    await asyncio.wait_for(dispatcher.flush_digest(), 1)
    assert notifier.digests == []
    statuses = await _statuses(engine)
    assert statuses[first][0] == "expired" and statuses[second][0] == "expired"


async def test_digest_rechecks_cancel_after_expired_result_write(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 12)
    dispatcher.enqueue(ids)
    await asyncio.wait_for(dispatcher.process_pending(), 5)
    first, second = ids[10:]
    async with engine.begin() as conn:
        await conn.execute(
            update(push_log)
            .where(push_log.c.id == first)
            .values(retry_until=clock.now - timedelta(seconds=1))
        )
    original_push_result = sink.push_result

    async def push_result_then_cancel(push_id: int, status: str, **kwargs):
        result = await original_push_result(push_id, status, **kwargs)
        if push_id == first and status == "expired":
            dispatcher.cancel([second])
        return result

    sink.push_result = push_result_then_cancel
    await asyncio.wait_for(dispatcher.flush_digest(), 1)
    assert notifier.digests == []
    assert second not in dispatcher._tracked and second not in dispatcher._cancelled


async def test_orphan_pending_recovered(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")  # 未入队的 pending（模拟异常遗漏）
    await dispatcher.retry_failed()
    assert notifier.events == []  # 刚创建的不算遗漏
    clock.advance(seconds=2 * settings.dispatch.digest_interval_s + 1)
    await dispatcher.retry_failed()
    assert len(notifier.events) == 1 and (await _statuses(engine))[pid][0] == "ok"


async def test_enqueue_dedupes(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid, pid])
    assert dispatcher._queue.qsize() == 1  # 同一批重复 id 只入队一次
    dispatcher.enqueue([pid])
    assert dispatcher._queue.qsize() == 1  # 已在跟踪中的记录不再入队
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and pid not in dispatcher._tracked  # 处理完成后解除跟踪


async def test_startup_loads_pending(engine, sink, settings, clock):
    notifier = FakeNotifier()
    [pid] = await _seed(engine, clock, "S")
    await _seed(engine, clock, "S", retry_minutes=-1)  # 已过期的不加载
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    await dispatcher.load_open()
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and (await _statuses(engine))[pid][0] == "ok"


async def test_start_restores_recent_individual_a_window(engine, sink, settings, clock):
    delivered_ids = await _seed(engine, clock, "A", 10, status="ok")
    async with engine.begin() as conn:
        await conn.execute(
            update(push_log)
            .where(push_log.c.id.in_(delivered_ids))
            .values(sent_at=clock.now - timedelta(minutes=4))
        )
    [pending_id] = await _seed(engine, clock, "A")
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=_idle_sleep)
    await dispatcher.start()
    try:
        async def wait_for_digest() -> None:
            while dispatcher.digest_size != 1:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_digest(), 1)
        assert notifier.events == []
        assert (await _statuses(engine))[pending_id][0] == "pending"
    finally:
        await asyncio.wait_for(dispatcher.stop(), 5)


async def test_start_ignores_individual_a_sent_before_window(engine, sink, settings, clock):
    delivered_ids = await _seed(engine, clock, "A", 10, status="dry_run")
    async with engine.begin() as conn:
        await conn.execute(
            update(push_log)
            .where(push_log.c.id.in_(delivered_ids))
            .values(sent_at=clock.now - timedelta(minutes=6))
        )
    [pending_id] = await _seed(engine, clock, "A")
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=_idle_sleep)
    await dispatcher.start()
    try:
        await _wait_status(engine, [pending_id], "ok", timeout=1)
        assert [card.push_id for card, _ in notifier.events] == [pending_id]
        assert dispatcher.digest_size == 0
    finally:
        await asyncio.wait_for(dispatcher.stop(), 5)


async def test_superseded_removed_from_digest(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 11)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert dispatcher.digest_size == 1
    outcome = IngestOutcome(collector="jin10", plans=[PushPlan(999, 1, 1, "S", "event", superseded_ids=[ids[10]])])
    dispatcher._on_ingest(outcome)  # 模拟 EventSink 提交后的通知
    assert dispatcher.digest_size == 0
    await dispatcher.flush_digest()
    assert notifier.digests == []


async def test_dry_run_status(engine, sink, settings, clock):
    dispatcher = _dispatcher(engine, sink, settings, clock, DryRunNotifier())
    [pid] = await _seed(engine, clock, "A")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert (await _statuses(engine))[pid][0] == "dry_run"


async def test_attach_receives_plans_from_sink(engine, sink, settings, clock):
    from tidewatch.models import Event, FetchResult

    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    dispatcher.attach()
    event = Event(source="jin10", category="news", event_type="flash", occurred_at=clock.now - timedelta(minutes=1),
                  title="美国9月非农就业人口增加8.2万人", dedupe_key="jin10:1", raw={"important": 1})
    await sink.ingest(FetchResult([event], None), collector="jin10")
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and notifier.events[0][0].level == "S"


# ---------- 补强：重试、汇总、取代、告警等级 ----------


async def test_persistent_retryable_failure_backs_off_then_fails(engine, sink, settings, clock):
    sleeps: list[float] = []
    notifier = FakeNotifier([SendResult(False, retryable=True, detail="code=11232")] * 4)
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleeps=sleeps)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert len(notifier.events) == 4  # 首次 + retry_immediate(3) 次重试
    assert sleeps == [1.0, 2.0, 4.0]
    assert (await _statuses(engine))[pid][0] == "failed"


async def test_digest_split_into_batches_of_30(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 41)  # 10 条单独发送，31 条进入汇总
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert len(notifier.events) == 10 and dispatcher.digest_size == 31
    await dispatcher.flush_digest()
    assert [len(d) for d in notifier.digests] == [30] and dispatcher.digest_size == 1
    await dispatcher.flush_digest()
    assert [len(d) for d in notifier.digests] == [30, 1] and dispatcher.digest_size == 0
    statuses = await _statuses(engine)
    assert all(statuses[i][0] == "ok" for i in ids)
    assert len({statuses[i][1] for i in ids[10:]}) == 2 and None not in {statuses[i][1] for i in ids[10:]}
    assert dispatcher._tracked == set()  # 汇总发完后不再跟踪


async def test_failed_digest_has_no_batch_id(engine, sink, settings, clock):
    notifier = FakeNotifier([SendResult(True)] * 10 + [SendResult(False, retryable=False, detail="code=19021")])
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 11)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    await dispatcher.flush_digest()
    assert (await _statuses(engine))[ids[10]] == ("failed", None)  # 没有发出去，不写 batch_id
    await dispatcher.retry_failed()  # 回到汇总缓冲
    await dispatcher.flush_digest()
    status, batch_id = (await _statuses(engine))[ids[10]]
    assert status == "ok" and batch_id is not None


async def test_dry_run_digest_status(engine, sink, settings, clock):
    dispatcher = _dispatcher(engine, sink, settings, clock, DryRunNotifier())
    ids = await _seed(engine, clock, "A", 11)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert dispatcher.digest_size == 1
    await dispatcher.flush_digest()
    statuses = await _statuses(engine)
    assert all(statuses[i][0] == "dry_run" for i in ids) and statuses[ids[10]][1] is not None


async def test_superseded_in_queue_not_sent(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid])
    dispatcher.cancel([pid])  # 仍在发送队列中（未进入汇总缓冲）时被取代
    await dispatcher.process_pending()
    assert notifier.events == []
    assert pid not in dispatcher._tracked and pid not in dispatcher._cancelled


async def test_superseded_while_loading_not_sent(engine, sink, settings, clock):
    """_load 期间被取代：加载完成后必须再检查一次，不能继续发送"""
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")
    original_load = dispatcher._load

    async def load_then_cancel(push_id: int):
        pending = await original_load(push_id)
        dispatcher.cancel([push_id])  # 模拟 _load 的 await 期间新事件取代了它
        return pending

    dispatcher._load = load_then_cancel
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert notifier.events == []
    assert pid not in dispatcher._tracked and pid not in dispatcher._cancelled


async def test_a_sent_individually_without_mention(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "A")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and notifier.events[0][1] is False


@pytest.mark.parametrize(("kind", "upgraded"), [("upgrade", True), ("event", False)])
async def test_upgrade_kind_marks_card_upgraded(engine, sink, settings, clock, kind, upgraded):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S", kind=kind)
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    card: EventCard = notifier.events[0][0]
    assert card.upgraded is upgraded


async def test_digest_buffered_a_not_resent_by_orphan_recovery(engine, sink, settings, clock):
    """汇总缓冲中的 A 级记录仍被跟踪：即使窗口已空出、也超过遗漏阈值，retry_failed 也不会单独重复发送"""
    notifier, mono = FakeNotifier(), FakeMonotonic()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, monotonic=mono)
    ids = await _seed(engine, clock, "A", 11)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert len(notifier.events) == 10 and dispatcher.digest_size == 1
    mono.value += settings.dispatch.a_window_s + 1  # 窗口已空出
    clock.advance(seconds=2 * settings.dispatch.digest_interval_s + 1)  # 超过遗漏阈值，但仍未过期
    await dispatcher.retry_failed()
    assert len(notifier.events) == 10 and dispatcher.digest_size == 1
    await dispatcher.flush_digest()
    assert len(notifier.events) == 10 and [len(d) for d in notifier.digests] == [1]
    assert (await _statuses(engine))[ids[10]][0] == "ok"


# ---------- 补强：容错与生命周期 ----------


class _RaiseOnFirstEvent(FakeNotifier):
    """第一次 send_event 抛出非预期异常，之后正常"""

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        result = await super().send_event(card, mention_all=mention_all)
        if len(self.events) == 1:
            raise ValueError("通知器内部错误")
        return result


async def test_retry_failed_continues_after_unexpected_error(engine, sink, settings, clock, caplog):
    notifier = _RaiseOnFirstEvent()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    first, second = await _seed(engine, clock, "S", 2, status="failed")
    with caplog.at_level(logging.ERROR):
        await dispatcher.retry_failed()  # 不应向外抛出
    statuses = await _statuses(engine)
    assert len(notifier.events) == 2  # 第一条出错后仍处理第二条
    assert statuses[first][0] == "failed" and statuses[second][0] == "ok"
    assert dispatcher._tracked == set()
    assert any(r.levelno == logging.ERROR for r in caplog.records)


async def test_start_stop_sends_pending_and_failed(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=_idle_sleep)
    [pending_id] = await _seed(engine, clock, "S")
    [failed_id] = await _seed(engine, clock, "A", status="failed")
    calls: list[str] = []
    load_open, retry_failed = dispatcher.load_open, dispatcher.retry_failed

    async def recorded_load_open() -> None:
        calls.append("load_open")
        await load_open()

    async def recorded_retry_failed() -> None:
        calls.append("retry_failed")
        await retry_failed()

    dispatcher.load_open, dispatcher.retry_failed = recorded_load_open, recorded_retry_failed
    await dispatcher.start()
    try:
        await _wait_status(engine, [pending_id, failed_id], "ok")
    finally:
        await asyncio.wait_for(dispatcher.stop(), 5)
    assert calls == ["load_open", "retry_failed"]  # 先恢复 pending，再处理 failed
    assert len(notifier.events) == 2
    assert dispatcher._tasks == []


async def test_stop_during_route_allows_startup_recovery(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=_idle_sleep)
    [pid] = await _seed(engine, clock, "S")
    route_started = asyncio.Event()
    release_route = asyncio.Event()
    original_route = dispatcher._route

    async def blocked_route(push_id: int) -> None:
        route_started.set()
        await release_route.wait()
        await original_route(push_id)

    dispatcher._route = blocked_route
    await dispatcher.start()
    await asyncio.wait_for(route_started.wait(), 1)
    await asyncio.wait_for(dispatcher.stop(), 5)

    dispatcher._route = original_route
    await dispatcher.start()
    try:
        await _wait_status(engine, [pid], "ok", timeout=1)
    finally:
        await asyncio.wait_for(dispatcher.stop(), 5)


async def test_cancel_after_send_queue_get_allows_startup_recovery(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=_idle_sleep)
    dispatcher._s_queue = _CancelAfterGetQueue()
    [pid] = await _seed(engine, clock, "S")
    await dispatcher.start()

    async def wait_for_s_worker_exit() -> None:
        while not any(task.get_name() == "dispatch-send-s" and task.done() for task in dispatcher._tasks):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait_for_s_worker_exit(), 1)
    await asyncio.wait_for(dispatcher.stop(), 5)

    dispatcher._s_queue = asyncio.Queue()
    await dispatcher.start()
    try:
        await _wait_status(engine, [pid], "ok", timeout=1)
    finally:
        await asyncio.wait_for(dispatcher.stop(), 5)


async def test_start_is_idempotent(engine, sink, settings, clock):
    dispatcher = _dispatcher(engine, sink, settings, clock, FakeNotifier(), sleep=_idle_sleep)
    await dispatcher.start()
    try:
        tasks = list(dispatcher._tasks)
        await dispatcher.start()  # 已启动：直接返回，不产生重复任务
        assert len(tasks) == 5 and dispatcher._tasks == tasks
    finally:
        await asyncio.wait_for(dispatcher.stop(), 5)
    assert dispatcher._tasks == [] and all(t.done() for t in tasks)
    await dispatcher.start()  # stop 之后可以再次启动
    try:
        assert len(dispatcher._tasks) == 5
    finally:
        await asyncio.wait_for(dispatcher.stop(), 5)


@pytest.mark.parametrize("broken", ["load_open", "retry_failed"])
async def test_start_creates_tasks_even_if_recovery_fails(engine, sink, settings, clock, caplog, broken):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleep=_idle_sleep)
    [pending_id] = await _seed(engine, clock, "S")
    [failed_id] = await _seed(engine, clock, "A", status="failed")

    async def boom() -> None:
        raise RuntimeError("数据库暂时不可用")

    setattr(dispatcher, broken, boom)
    with caplog.at_level(logging.ERROR):
        await dispatcher.start()  # 启动恢复出错不能阻止后台任务创建
    try:
        assert len(dispatcher._tasks) == 5 and not any(t.done() for t in dispatcher._tasks)
        assert any(r.levelno == logging.ERROR for r in caplog.records)
        # 出错的一步之外，另一步照常完成
        if broken == "load_open":
            await _wait_status(engine, [failed_id], "ok")
            assert (await _statuses(engine))[pending_id][0] == "pending"
        else:
            await _wait_status(engine, [pending_id], "ok")
            assert (await _statuses(engine))[failed_id][0] == "failed"
        # 后台发送任务仍在工作
        [new_id] = await _seed(engine, clock, "S")
        dispatcher.enqueue([new_id])
        await _wait_status(engine, [new_id], "ok")
    finally:
        await asyncio.wait_for(dispatcher.stop(), 5)


# ---------- cancel 的跟踪语义 ----------


async def test_cancel_untracked_id_is_not_remembered(engine, sink, settings, clock):
    """未跟踪的 id（数据库中已是 superseded，重试循环不会再取到）不加入 _cancelled，避免集合无限增长"""
    dispatcher = _dispatcher(engine, sink, settings, clock, FakeNotifier())
    dispatcher.cancel([12345, 67890])
    assert dispatcher._cancelled == set() and dispatcher._tracked == set()


async def test_cancel_digest_id_untracks_and_leaves_no_mark(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 12)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert dispatcher.digest_size == 2
    dispatcher.cancel([ids[10]])  # 汇总缓冲中的：直接移除
    assert dispatcher._cancelled == set() and ids[10] not in dispatcher._tracked
    assert ids[11] in dispatcher._tracked and dispatcher.digest_size == 1
    await dispatcher.flush_digest()
    assert [[c.push_id for c in cards] for cards in notifier.digests] == [[ids[11]]]
    assert dispatcher._tracked == set() and dispatcher._cancelled == set()


async def test_cancel_queued_id_keeps_mark_until_handled(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [queued, other] = await _seed(engine, clock, "S", 2)
    dispatcher.enqueue([queued, other])
    dispatcher.cancel([queued, 424242])  # 一个在发送队列中，一个未被跟踪
    assert dispatcher._cancelled == {queued}
    await dispatcher.process_pending()
    assert [card.push_id for card, _ in notifier.events] == [other]  # 队列中被取代的不发送
    assert dispatcher._cancelled == set() and dispatcher._tracked == set()


async def test_cancel_during_flush_not_sent_and_no_leftover(engine, sink, settings, clock):
    """汇总发送过程中（已从缓冲取出、正在加载）被取代的推送不发送，也不遗留标记"""
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 13)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert dispatcher.digest_size == 3
    original_load = dispatcher._load

    async def load_then_cancel(push_id: int):
        pending = await original_load(push_id)
        if push_id == ids[10]:
            dispatcher.cancel([ids[11]])  # 加载第一条期间，第二条被新事件取代
        return pending

    dispatcher._load = load_then_cancel
    await dispatcher.flush_digest()
    assert [[c.push_id for c in cards] for cards in notifier.digests] == [[ids[10], ids[12]]]
    assert dispatcher._tracked == set() and dispatcher._cancelled == set()
