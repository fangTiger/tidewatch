from datetime import timedelta

import pytest
from sqlalchemy import insert, select, update

from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.tables import event_clusters, events, push_log


@pytest.fixture
async def sink(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    yield event_sink
    await event_sink.stop()


async def _one_push(sink, engine, clock) -> int:
    event = Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=clock.now - timedelta(minutes=1),
        title="美国9月非农就业人口增加8.2万人",
        dedupe_key="jin10:1",
        raw={"important": 1},
    )
    await sink.ingest(FetchResult([event], None), collector="jin10")
    async with engine.connect() as conn:
        return (await conn.execute(select(push_log.c.id))).scalar_one()


async def test_ok_updates_delivered_level(sink, engine, clock):
    seen = []
    sink.on_push_result(seen.append)
    push_id = await _one_push(sink, engine, clock)
    assert await sink.push_result(push_id, "ok", response="code=0") == "ok"
    async with engine.connect() as conn:
        row = (await conn.execute(select(push_log))).mappings().one()
        delivered = (await conn.execute(select(event_clusters.c.delivered_level))).scalar_one()
    assert row["status"] == "ok" and row["attempts"] == 1 and row["sent_at"] == clock.now
    assert row["response"] == "code=0" and delivered == "S"
    assert seen[0].status == "ok" and seen[0].push_id == push_id


async def test_superseded_keeps_status_but_records_delivery(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    async with engine.begin() as conn:
        await conn.execute(update(push_log).values(status="superseded"))
    assert await sink.push_result(push_id, "ok") == "superseded"
    async with engine.connect() as conn:
        delivered = (await conn.execute(select(event_clusters.c.delivered_level))).scalar_one()
    assert delivered == "S"


async def test_failed_then_dry_run_with_batch(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    assert await sink.push_result(push_id, "failed", response="超时") == "failed"
    assert await sink.push_result(push_id, "dry_run", batch_id="b1") == "dry_run"
    async with engine.connect() as conn:
        row = (await conn.execute(select(push_log))).mappings().one()
    assert row["attempts"] == 2 and row["batch_id"] == "b1" and row["status"] == "dry_run"


async def test_terminal_status_not_overwritten(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    await sink.push_result(push_id, "ok")
    assert await sink.push_result(push_id, "failed") == "ok"
    async with engine.connect() as conn:
        assert (await conn.execute(select(push_log.c.attempts))).scalar_one() == 1


async def test_expired_does_not_count_attempt(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    assert await sink.push_result(push_id, "expired") == "expired"
    async with engine.connect() as conn:
        assert (await conn.execute(select(push_log.c.attempts))).scalar_one() == 0


async def test_invalid_status_rejected(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    with pytest.raises(ValueError):
        await sink.push_result(push_id, "superseded")


async def test_unknown_push_id(sink):
    assert await sink.push_result(999999, "ok") is None


async def _insert_push(engine, clock, *, level: str, cluster_delivered: str | None = None, status: str = "pending") -> int:
    """直接构造簇、事件与推送记录，返回推送记录 id"""
    async with engine.begin() as conn:
        cluster_id = (
            await conn.execute(
                insert(event_clusters)
                .values(category="news", first_seen_at=clock.now, last_seen_at=clock.now, max_level=level,
                        committed_level=level, delivered_level=cluster_delivered)
                .returning(event_clusters.c.id)
            )
        ).scalar_one()
        event_id = (
            await conn.execute(
                insert(events)
                .values(source="jin10", category="news", event_type="flash", occurred_at=clock.now,
                        collected_at=clock.now, title="标题", dedupe_key="manual:1", final_level=level,
                        rule_level=level, cluster_id=cluster_id)
                .returning(events.c.id)
            )
        ).scalar_one()
        return (
            await conn.execute(
                insert(push_log)
                .values(cluster_id=cluster_id, event_id=event_id, level=level, kind="event", status=status,
                        retry_until=clock.now + timedelta(hours=1), created_at=clock.now)
                .returning(push_log.c.id)
            )
        ).scalar_one()


async def test_delivered_level_keeps_higher_level(sink, engine, clock):
    """簇已送达 S 后，A 级推送回写 ok 不会把 delivered_level 降为 A"""
    push_id = await _insert_push(engine, clock, level="A", cluster_delivered="S")
    assert await sink.push_result(push_id, "ok") == "ok"
    async with engine.connect() as conn:
        delivered = (await conn.execute(select(event_clusters.c.delivered_level))).scalar_one()
    assert delivered == "S"


async def test_delivered_level_raised_by_higher_push(sink, engine, clock):
    """反向确认：已送达 A 的簇收到 S 级推送成功后升为 S"""
    push_id = await _insert_push(engine, clock, level="S", cluster_delivered="A")
    await sink.push_result(push_id, "dry_run")
    async with engine.connect() as conn:
        delivered = (await conn.execute(select(event_clusters.c.delivered_level))).scalar_one()
    assert delivered == "S"


async def test_superseded_record_not_modified_by_late_result(sink, engine, clock):
    """被取代的记录收到迟到的成功结果：状态、attempts、sent_at 都不变"""
    push_id = await _one_push(sink, engine, clock)
    async with engine.begin() as conn:
        await conn.execute(update(push_log).values(status="superseded"))
    clock.advance(minutes=1)
    assert await sink.push_result(push_id, "ok", response="迟到", batch_id="late") == "superseded"
    async with engine.connect() as conn:
        row = (await conn.execute(select(push_log))).mappings().one()
    assert row["status"] == "superseded" and row["attempts"] == 0 and row["sent_at"] is None
    assert row["response"] is None and row["batch_id"] is None


async def test_response_truncated_to_500_chars(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    await sink.push_result(push_id, "failed", response="错" * 800)
    async with engine.connect() as conn:
        response = (await conn.execute(select(push_log.c.response))).scalar_one()
    assert response == "错" * 500


async def test_unknown_push_id_does_not_notify(sink):
    seen = []
    sink.on_push_result(seen.append)
    assert await sink.push_result(999999, "ok") is None
    assert seen == []


async def test_invalid_status_rejected_before_enqueue(engine, settings, clock):
    """状态非法时在入队前就抛 ValueError（未启动的 EventSink 也是 ValueError，而不是 RuntimeError）"""
    event_sink = create_event_sink(engine, settings, clock=clock)
    with pytest.raises(ValueError, match="非法的推送结果状态"):
        await event_sink.push_result(1, "superseded")


def test_status_constants_importable_from_sink():
    """调度器从 sink 模块导入这两个常量，必须与 repo 中的定义保持同一份"""
    from tidewatch.pipeline.sink import DELIVERED_STATUSES, OPEN_STATUSES
    from tidewatch.store import repo

    assert OPEN_STATUSES is repo.OPEN_STATUSES and OPEN_STATUSES == ("pending", "failed")
    assert DELIVERED_STATUSES is repo.DELIVERED_STATUSES and DELIVERED_STATUSES == ("ok", "dry_run")
