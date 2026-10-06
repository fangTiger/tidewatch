"""Hyperliquid 当前仓位快照的解析与持久化。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import delete, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.store.tables import hl_positions, hl_wallets


def _decimal(value: Any, field: str, *, required: bool = False) -> Decimal | None:
    if value is None and not required:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"clearinghouseState.{field} 无效") from exc
    if not parsed.is_finite():
        raise ValueError(f"clearinghouseState.{field} 无效")
    return parsed


def _position_rows(
    response: Any,
    *,
    address: str,
    dex: str,
    updated_at: datetime,
) -> list[dict[str, Any]]:
    if not isinstance(response, Mapping):
        raise ValueError("clearinghouseState 响应必须是对象")
    asset_positions = response.get("assetPositions")
    if not isinstance(asset_positions, list):
        raise ValueError("clearinghouseState.assetPositions 必须是数组")

    by_coin: dict[str, dict[str, Any]] = {}
    for item in asset_positions:
        position = item.get("position") if isinstance(item, Mapping) else None
        if not isinstance(position, Mapping):
            raise ValueError("clearinghouseState.position 必须是对象")
        coin = position.get("coin")
        if not isinstance(coin, str) or not coin:
            raise ValueError("clearinghouseState.position.coin 必须是非空字符串")
        leverage = position.get("leverage")
        leverage_value = leverage.get("value") if isinstance(leverage, Mapping) else leverage
        notional = _decimal(position.get("positionValue"), "position.positionValue")
        by_coin[coin] = {
            "address": address,
            "dex": dex,
            "coin": coin,
            "szi": _decimal(position.get("szi"), "position.szi", required=True),
            "entry_px": _decimal(position.get("entryPx"), "position.entryPx"),
            "liquidation_px": _decimal(
                position.get("liquidationPx"),
                "position.liquidationPx",
            ),
            "leverage": _decimal(leverage_value, "position.leverage.value"),
            "notional_usd": abs(notional) if notional is not None else None,
            "unrealized_pnl": _decimal(
                position.get("unrealizedPnl"),
                "position.unrealizedPnl",
            ),
            "updated_at": updated_at,
        }
    return list(by_coin.values())


async def upsert_position_snapshot(
    conn: AsyncConnection,
    *,
    address: str,
    dex: str,
    response: Any,
    updated_at: datetime,
) -> int:
    """原子替换一个地址在一个 dex 下的当前仓位快照。"""
    rows = _position_rows(
        response,
        address=address,
        dex=dex,
        updated_at=updated_at,
    )
    if rows:
        stmt = pg_insert(hl_positions).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["address", "dex", "coin"],
            set_={
                "szi": stmt.excluded.szi,
                "entry_px": stmt.excluded.entry_px,
                "liquidation_px": stmt.excluded.liquidation_px,
                "leverage": stmt.excluded.leverage,
                "notional_usd": stmt.excluded.notional_usd,
                "unrealized_pnl": stmt.excluded.unrealized_pnl,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        await conn.execute(stmt)

    stale = delete(hl_positions).where(
        hl_positions.c.address == address,
        hl_positions.c.dex == dex,
    )
    if rows:
        stale = stale.where(hl_positions.c.coin.not_in([row["coin"] for row in rows]))
    await conn.execute(stale)

    if rows:
        await conn.execute(
            update(hl_wallets)
            .where(hl_wallets.c.address == address)
            .values(last_position_at=updated_at)
        )
    return len(rows)
