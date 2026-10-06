"""Hyperliquid 钱包监控基线的持久化与失效处理。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.store.tables import hl_state


def baseline_state_name(address: str) -> str:
    """返回钱包监控基线在 ``hl_state`` 中的名称。"""
    return f"monitor_baseline:{address}"


async def load_monitor_baseline(
    conn: AsyncConnection,
    address: str,
) -> datetime | None:
    """读取钱包监控基线时刻；缺失或损坏时视为尚未建立。"""
    cursor = await conn.scalar(
        select(hl_state.c.cursor).where(hl_state.c.name == baseline_state_name(address))
    )
    if not isinstance(cursor, Mapping):
        return None
    value = cursor.get("established_at")
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else None


async def load_last_observed_at(
    conn: AsyncConnection,
    address: str,
) -> datetime | None:
    """读取钱包最近一次完整状态观察时刻，旧游标回退到基线时刻。"""
    cursor = await conn.scalar(
        select(hl_state.c.cursor).where(hl_state.c.name == baseline_state_name(address))
    )
    if not isinstance(cursor, Mapping):
        return None
    value = cursor.get("last_observed_at") or cursor.get("established_at")
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else None


async def save_monitor_baseline(
    conn: AsyncConnection,
    address: str,
    established_at: datetime,
) -> None:
    """幂等保存钱包监控基线时刻。"""
    if established_at.tzinfo is None:
        raise ValueError("监控基线时间必须带时区")
    stmt = pg_insert(hl_state).values(
        name=baseline_state_name(address),
        cursor={
            "established_at": established_at.isoformat(),
            "last_observed_at": established_at.isoformat(),
        },
        updated_at=established_at,
    )
    await conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
        )
    )


async def save_last_observed_at(
    conn: AsyncConnection,
    address: str,
    established_at: datetime,
    observed_at: datetime,
) -> None:
    """保留基线时刻并推进钱包最近一次完整状态观察时刻。"""
    if established_at.tzinfo is None or observed_at.tzinfo is None:
        raise ValueError("监控观察时间必须带时区")
    stmt = pg_insert(hl_state).values(
        name=baseline_state_name(address),
        cursor={
            "established_at": established_at.isoformat(),
            "last_observed_at": observed_at.isoformat(),
        },
        updated_at=observed_at,
    )
    await conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
        )
    )


async def invalidate_monitor_baseline(conn: AsyncConnection, address: str) -> None:
    """使钱包下次监控观察重新静默建基线，并丢弃旧补查/滞回游标。"""
    normalized = address
    await conn.execute(
        delete(hl_state).where(
            hl_state.c.name.in_(
                (
                    baseline_state_name(normalized),
                    f"fills:{normalized}",
                )
            )
            | hl_state.c.name.like(f"liq_risk:{normalized}:%")
        )
    )
