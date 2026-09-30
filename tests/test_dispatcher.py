import uuid
from datetime import timedelta

import pytest
from sqlalchemy import insert, select

from fakes import FakeNotifier
from tidewatch.notify.base import SendResult
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


@pytest.fixture
async def sink(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    yield event_sink
    await event_sink.stop()


async def _seed(engine, clock, level: str, n: int = 1, *, status: str = "pending", retry_minutes: int = 30) -> list[int]:
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
                cluster_id=cid, event_id=eid, level=level, kind="event", status=status,
                retry_until=clock.now + timedelta(minutes=retry_minutes), created_at=clock.now,
            ).returning(push_log.c.id))).scalar_one()
            ids.append(pid)
    return ids


def _dispatcher(engine, sink, settings, clock, notifier, *, monotonic=None, sleeps=None) -> Dispatcher:
    async def fake_sleep(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    return Dispatcher(engine, sink, notifier, settings.dispatch, clock=clock, sleep=fake_sleep,
                      monotonic=monotonic or FakeMonotonic())


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
    await dispatcher.process_pending()
    assert len(notifier.events) == 1


async def test_startup_loads_pending(engine, sink, settings, clock):
    notifier = FakeNotifier()
    [pid] = await _seed(engine, clock, "S")
    await _seed(engine, clock, "S", retry_minutes=-1)  # 已过期的不加载
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    await dispatcher.load_open()
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and (await _statuses(engine))[pid][0] == "ok"


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
