from datetime import timedelta

import pytest
from sqlalchemy import select, update

from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.tables import event_clusters, push_log


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
