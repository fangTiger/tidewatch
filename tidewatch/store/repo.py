"""通用 SQL：事件插入与分级回写、采集器游标"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.models import Event
from tidewatch.store.tables import collector_state, events

BODY_LIMIT = 2000


async def insert_event(conn: AsyncConnection, event: Event, now: datetime) -> int | None:
    """插入事件；dedupe_key 已存在时不做修改并返回 None

    插入成功时回填 event.id 与 event.collected_at。
    """
    stmt = (
        pg_insert(events)
        .values(
            source=event.source,
            category=event.category,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            ref_date=event.ref_date,
            collected_at=now,
            title=event.title,
            body=(event.body or "")[:BODY_LIMIT],
            url=event.url,
            assets=list(event.assets),
            amount_usd=event.amount_usd,
            dedupe_key=event.dedupe_key,
            raw=event.raw,
        )
        .on_conflict_do_nothing(index_elements=["dedupe_key"])
        .returning(events.c.id)
    )
    event_id = (await conn.execute(stmt)).scalar_one_or_none()
    if event_id is not None:
        event.id = event_id
        event.collected_at = now
    return event_id


async def save_grading(conn: AsyncConnection, event: Event) -> None:
    """回写分级结果与 AI 状态"""
    await conn.execute(
        update(events)
        .where(events.c.id == event.id)
        .values(
            rule_level=event.rule_level,
            rule_hits=list(event.rule_hits),
            final_level=event.final_level,
            ai_status=event.ai_status,
        )
    )


async def load_cursor(conn: AsyncConnection, name: str) -> dict[str, Any] | None:
    """读取采集器游标，不存在时返回 None"""
    stmt = select(collector_state.c.cursor).where(collector_state.c.name == name)
    return (await conn.execute(stmt)).scalar_one_or_none()


async def save_cursor(
    conn: AsyncConnection,
    name: str,
    cursor: dict[str, Any] | None,
    now: datetime,
    *,
    had_events: bool,
) -> None:
    """写入采集器游标；cursor 为 None 时保留原游标；had_events 为真时更新 last_event_at"""
    values: dict[str, Any] = {"name": name, "updated_at": now}
    if cursor is not None:
        values["cursor"] = cursor
    if had_events:
        values["last_event_at"] = now
    stmt = pg_insert(collector_state).values(**values)
    set_ = {key: stmt.excluded[key] for key in values if key != "name"}
    await conn.execute(stmt.on_conflict_do_update(index_elements=["name"], set_=set_))
