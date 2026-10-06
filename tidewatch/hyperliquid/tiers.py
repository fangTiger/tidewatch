"""巨鲸三层名单的持仓重算与断点游标。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.hyperliquid.positions import upsert_position_snapshot
from tidewatch.hyperliquid.registry import REGISTRY_LOCK_KEY
from tidewatch.store.tables import hl_positions, hl_state, hl_wallets


STATE_NAME = "tier_recompute"
NON_PROMOTABLE_STYLES = frozenset({"market_maker", "high_frequency"})


@dataclass(frozen=True)
class TierResult:
    """一次分层重算的摘要。"""

    queried: int
    promoted: int
    demoted: int


class TierRecalculator:
    """使用批处理预算查询候选持仓，再确定性应用重点层上限。"""

    def __init__(
        self,
        engine: AsyncEngine,
        rest: Any,
        *,
        minimum_notional: Decimal = Decimal("1000000"),
        focus_limit: int = 200,
        no_position_days: int = 7,
        clock: Clock = utcnow,
    ) -> None:
        self.engine = engine
        self.rest = rest
        self.minimum_notional = minimum_notional
        self.focus_limit = focus_limit
        self.no_position_days = no_position_days
        self.clock = clock

    async def recompute(self) -> TierResult:
        """查询全部可自动晋升候选并重算；失败时保留查询断点。"""
        async with self.engine.connect() as conn:
            wallet_rows = (await conn.execute(select(hl_wallets))).mappings().all()
            state = (
                await conn.execute(select(hl_state.c.cursor).where(hl_state.c.name == STATE_NAME))
            ).scalar_one_or_none()

        candidates = sorted(
            row["address"]
            for row in wallet_rows
            if row["tier"] == "candidate"
            and not row["excluded"]
            and row["style"] not in NON_PROMOTABLE_STYLES
        )
        notionals: dict[str, Decimal] = {}
        next_index = 0
        if isinstance(state, dict) and state.get("phase") == "querying" and state.get("addresses") == candidates:
            next_index = int(state.get("next_index", 0))
            notionals = {
                address: Decimal(value)
                for address, value in dict(state.get("notionals", {})).items()
                if address in candidates
            }
        else:
            await self._write_query_cursor(candidates, 0, notionals)

        for index in range(next_index, len(candidates)):
            address = candidates[index]
            response = await self.rest.info(
                "clearinghouseState",
                budget="batch",
                user=address,
            )
            notional = self._total_notional(response)
            async with self.engine.begin() as conn:
                await upsert_position_snapshot(
                    conn,
                    address=address,
                    dex="",
                    response=response,
                    updated_at=self.clock(),
                )
            notionals[address] = notional
            await self._write_query_cursor(candidates, index + 1, notionals)

        return await self._apply(notionals, queried=len(candidates))

    @staticmethod
    def _total_notional(response: Any) -> Decimal:
        if not isinstance(response, Mapping):
            raise ValueError("clearinghouseState 响应必须是对象")
        summary = response.get("marginSummary")
        raw = summary.get("totalNtlPos") if isinstance(summary, Mapping) else None
        if raw is None:
            total = Decimal(0)
            positions = response.get("assetPositions", [])
            if not isinstance(positions, list):
                raise ValueError("clearinghouseState.assetPositions 必须是数组")
            for item in positions:
                position = item.get("position", {}) if isinstance(item, Mapping) else {}
                value = position.get("positionValue") if isinstance(position, Mapping) else None
                if value is not None:
                    total += abs(Decimal(str(value)))
            return total
        try:
            value = abs(Decimal(str(raw)))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("clearinghouseState.totalNtlPos 无效") from exc
        if not value.is_finite():
            raise ValueError("clearinghouseState.totalNtlPos 无效")
        return value

    async def _apply(
        self,
        candidate_notionals: Mapping[str, Decimal],
        *,
        queried: int,
    ) -> TierResult:
        now = self.clock()
        cutoff = now - timedelta(days=self.no_position_days)
        async with self.engine.begin() as conn:
            await conn.execute(select(func.pg_advisory_xact_lock(REGISTRY_LOCK_KEY)))
            wallet_rows = (
                await conn.execute(select(hl_wallets).with_for_update())
            ).mappings().all()
            position_rows = (
                await conn.execute(
                    select(
                        hl_positions.c.address,
                        func.coalesce(func.sum(func.abs(hl_positions.c.notional_usd)), 0),
                    ).group_by(hl_positions.c.address)
                )
            ).all()
            current_notionals = {address: Decimal(value or 0) for address, value in position_rows}

            protected: set[str] = set()
            automatic_pool: dict[str, Decimal] = {}
            for row in wallet_rows:
                address = row["address"]
                if row["excluded"] or row["tier"] == "core":
                    continue
                profile = row["profile"] or {}
                if row["tier"] == "focus" and (
                    row["source"] == "manual" or profile.get("manual_focus")
                ):
                    protected.add(address)
                    continue
                if row["tier"] == "focus":
                    if row["style"] in NON_PROMOTABLE_STYLES:
                        continue
                    notional = current_notionals.get(address, Decimal(0))
                    stale = (
                        notional == 0
                        and row["last_position_at"] is not None
                        and row["last_position_at"] <= cutoff
                    )
                    if not stale:
                        automatic_pool[address] = notional

            for row in wallet_rows:
                address = row["address"]
                if (
                    row["tier"] == "candidate"
                    and not row["excluded"]
                    and row["style"] not in NON_PROMOTABLE_STYLES
                    and candidate_notionals.get(address, Decimal(0)) >= self.minimum_notional
                ):
                    automatic_pool[address] = candidate_notionals[address]

            available = max(0, self.focus_limit - len(protected))
            selected_auto = {
                address
                for address, _ in sorted(
                    automatic_pool.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:available]
            }
            selected_focus = protected | selected_auto
            promoted = 0
            demoted = 0
            for row in wallet_rows:
                address = row["address"]
                old_tier = row["tier"]
                if row["excluded"]:
                    new_tier = None
                elif old_tier == "core":
                    new_tier = "core"
                elif address in selected_focus:
                    new_tier = "focus"
                else:
                    new_tier = "candidate"
                values: dict[str, Any] = {"tier": new_tier}
                observed = candidate_notionals.get(address, current_notionals.get(address, Decimal(0)))
                if observed > 0:
                    values["last_position_at"] = now
                if old_tier != "focus" and new_tier == "focus":
                    promoted += 1
                elif old_tier == "focus" and new_tier != "focus":
                    demoted += 1
                await conn.execute(
                    update(hl_wallets).where(hl_wallets.c.address == address).values(**values)
                )

            cursor = {
                "phase": "complete",
                "completed_at": now.isoformat(),
                "processed": queried,
                "promoted": promoted,
                "demoted": demoted,
            }
            stmt = pg_insert(hl_state).values(name=STATE_NAME, cursor=cursor, updated_at=now)
            stmt = stmt.on_conflict_do_update(
                index_elements=["name"],
                set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
            )
            await conn.execute(stmt)
        return TierResult(queried, promoted, demoted)

    async def _write_query_cursor(
        self,
        addresses: list[str],
        next_index: int,
        notionals: Mapping[str, Decimal],
    ) -> None:
        now = self.clock()
        cursor = {
            "phase": "querying",
            "addresses": addresses,
            "next_index": next_index,
            "notionals": {address: str(value) for address, value in notionals.items()},
        }
        stmt = pg_insert(hl_state).values(name=STATE_NAME, cursor=cursor, updated_at=now)
        stmt = stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
        )
        async with self.engine.begin() as conn:
            await conn.execute(stmt)
