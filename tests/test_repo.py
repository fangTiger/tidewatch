from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from tidewatch.models import Event
from tidewatch.store.repo import insert_event, load_cursor, save_cursor, save_grading
from tidewatch.store.tables import collector_state, events

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


def _event(key: str, body: str = "") -> Event:
    return Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=NOW - timedelta(minutes=1),
        title="标题",
        body=body,
        dedupe_key=key,
        assets=["BTC"],
        raw={"important": 1},
    )


async def test_insert_event_and_duplicate(engine):
    event = _event("jin10:1", body="长" * 2500)
    async with engine.begin() as conn:
        first = await insert_event(conn, event, NOW)
        second = await insert_event(conn, _event("jin10:1"), NOW)
        row = (await conn.execute(select(events).where(events.c.id == first))).mappings().one()
    assert first is not None and second is None
    assert event.id == first and event.collected_at == NOW
    assert len(row["body"]) == 2000
    assert row["assets"] == ["BTC"] and row["collected_at"] == NOW


async def test_save_grading(engine):
    event = _event("jin10:2")
    async with engine.begin() as conn:
        await insert_event(conn, event, NOW)
        event.rule_level, event.rule_hits, event.final_level, event.ai_status = "A", ["x"], "A", "skipped"
        await save_grading(conn, event)
        row = (await conn.execute(select(events).where(events.c.id == event.id))).mappings().one()
    assert (row["rule_level"], row["rule_hits"], row["final_level"], row["ai_status"]) == ("A", ["x"], "A", "skipped")


async def test_cursor_roundtrip(engine):
    async with engine.begin() as conn:
        assert await load_cursor(conn, "jin10") is None
        await save_cursor(conn, "jin10", {"last_id": "1"}, NOW, had_events=True)
        await save_cursor(conn, "jin10", None, NOW + timedelta(minutes=1), had_events=False)
        assert await load_cursor(conn, "jin10") == {"last_id": "1"}  # cursor 为 None 时不覆盖
        row = (await conn.execute(select(collector_state))).mappings().one()
    assert row["last_event_at"] == NOW
    assert row["updated_at"] == NOW + timedelta(minutes=1)


async def test_save_cursor_conflict_updates_cursor_and_last_event_at(engine):
    """冲突分支：第二次传入新 cursor 且 had_events=True，应写入新 cursor 与新的 last_event_at"""
    later = NOW + timedelta(minutes=5)
    async with engine.begin() as conn:
        await save_cursor(conn, "jin10", {"last_id": "1"}, NOW, had_events=True)
        await save_cursor(conn, "jin10", {"last_id": "2"}, later, had_events=True)
        assert await load_cursor(conn, "jin10") == {"last_id": "2"}
        row = (await conn.execute(select(collector_state))).mappings().one()
    assert row["last_event_at"] == later and row["updated_at"] == later


async def test_insert_event_normalizes_float_amount(engine):
    """float 金额按其十进制字面值入库，不引入二进制浮点误差"""
    event = _event("jin10:amount")
    event.amount_usd = 0.1
    async with engine.begin() as conn:
        await insert_event(conn, event, NOW)
        amount = (await conn.execute(select(events.c.amount_usd).where(events.c.id == event.id))).scalar_one()
    assert amount == Decimal("0.1")
    assert str(amount) == "0.1"


async def test_save_grading_requires_saved_event(engine):
    async with engine.begin() as conn:
        with pytest.raises(ValueError, match="事件尚未入库"):
            await save_grading(conn, _event("jin10:unsaved"))
