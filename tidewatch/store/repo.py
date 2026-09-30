"""通用 SQL：事件插入与分级回写、采集器游标"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.models import Event, max_level
from tidewatch.store.tables import collector_state, event_clusters, events, push_log

BODY_LIMIT = 2000
PUSH_RESULT_STATUSES = ("ok", "failed", "dry_run", "expired")


async def insert_event(conn: AsyncConnection, event: Event, now: datetime) -> int | None:
    """插入事件；dedupe_key 已存在时不做修改并返回 None

    插入成功时回填 event.id 与 event.collected_at。
    amount_usd 为 float 时先按十进制字面值归一化（0.1 存为 0.1，而不是 0.1000000000000000055…）。
    """
    amount_usd = event.amount_usd
    if isinstance(amount_usd, float):
        amount_usd = Decimal(str(amount_usd))
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
            amount_usd=amount_usd,
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
    """回写分级结果与 AI 状态；事件必须已入库（id 不为 None）"""
    if event.id is None:
        raise ValueError("事件尚未入库")
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


async def record_push_result(
    conn: AsyncConnection,
    push_id: int,
    status: str,
    now: datetime,
    *,
    response: str | None = None,
    batch_id: str | None = None,
) -> tuple[str, int] | None:
    """回写一次推送结果，返回 (最终状态, 簇 id)；记录不存在时返回 None"""
    if status not in PUSH_RESULT_STATUSES:
        raise ValueError(f"非法的推送结果状态：{status}")
    row = (
        await conn.execute(
            select(push_log.c.status, push_log.c.cluster_id, push_log.c.level)
            .where(push_log.c.id == push_id)
            .with_for_update()
        )
    ).one_or_none()
    if row is None:
        return None
    delivered = status in ("ok", "dry_run")
    final_status = row.status
    if row.status in ("pending", "failed"):
        final_status = status
        values: dict[str, Any] = {"status": status}
        if status != "expired":
            values["attempts"] = push_log.c.attempts + 1
        if response is not None:
            values["response"] = response[:500]
        if batch_id is not None:
            values["batch_id"] = batch_id
        if delivered:
            values["sent_at"] = now
        await conn.execute(update(push_log).where(push_log.c.id == push_id).values(**values))
    if delivered:
        current = (
            await conn.execute(select(event_clusters.c.delivered_level).where(event_clusters.c.id == row.cluster_id))
        ).scalar_one()
        await conn.execute(
            update(event_clusters)
            .where(event_clusters.c.id == row.cluster_id)
            .values(delivered_level=max_level(current, row.level))
        )
    return final_status, row.cluster_id
