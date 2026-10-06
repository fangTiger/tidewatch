"""Hyperliquid 巨鲸运行时监控：优先对账、快照、成交与实时价格。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import IntEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.hyperliquid.detect import (
    PositionSnapshot,
    detect_events_with_state,
    load_liquidation_risk_state,
    save_liquidation_risk_state,
)
from tidewatch.hyperliquid.positions import upsert_position_snapshot
from tidewatch.hyperliquid.registry import normalize_address
from tidewatch.models import FetchResult
from tidewatch.store.tables import hl_fills, hl_positions, hl_state, hl_wallets


logger = logging.getLogger(__name__)
_MISSING = object()


class ReconcilePriority(IntEnum):
    """数值越小，请求优先级越高。"""

    TRADE = 0
    LIQUIDATION = 1
    HOT = 2
    NORMAL = 3


@dataclass(frozen=True)
class ScheduledReconcile:
    address: str
    priority: ReconcilePriority
    due_at: float
    reason: str


class PriorityReconcileQueue:
    """按钱包去重的对账队列；预算不足时任务可原样顺延。"""

    def __init__(self) -> None:
        self._tasks: dict[str, ScheduledReconcile] = {}

    def __len__(self) -> int:
        return len(self._tasks)

    def __contains__(self, address: object) -> bool:
        if not isinstance(address, str):
            return False
        try:
            normalized = normalize_address(address)
        except ValueError:
            return False
        return normalized in self._tasks

    def schedule(
        self,
        address: str,
        priority: ReconcilePriority,
        *,
        due_at: float,
        reason: str,
    ) -> None:
        normalized = normalize_address(address)
        incoming = ScheduledReconcile(normalized, priority, due_at, reason)
        current = self._tasks.get(normalized)
        if current is None:
            self._tasks[normalized] = incoming
            return
        if priority < current.priority:
            self._tasks[normalized] = incoming
            return
        if priority == current.priority:
            if priority is ReconcilePriority.TRADE:
                self._tasks[normalized] = incoming
            elif due_at < current.due_at:
                self._tasks[normalized] = incoming

    def pop_ready(self, now: float) -> ScheduledReconcile | None:
        ready = [task for task in self._tasks.values() if task.due_at <= now]
        if not ready:
            return None
        task = min(ready, key=lambda item: (item.priority, item.due_at, item.address))
        del self._tasks[task.address]
        return task

    def defer(self, task: ScheduledReconcile, *, due_at: float) -> None:
        self.schedule(task.address, task.priority, due_at=due_at, reason=task.reason)


@dataclass(frozen=True)
class HotWallet:
    address: str
    liquidation_distance: Decimal | None
    max_notional_usd: Decimal


@dataclass
class _ReconcileProgress:
    """一次多请求对账的已完成步骤，预算顺延后从断点继续。"""

    observed_at: datetime
    dexes: tuple[str, ...]
    states: dict[str, Any]
    abstraction: Any = _MISSING
    spot: Any = _MISSING


def select_hot_wallets(
    rows: Iterable[Mapping[str, Any]],
    mids: Mapping[str, Decimal],
    cfg: Any,
) -> list[HotWallet]:
    """按最近强平距离、再按最大仓位名义选出热点钱包。"""
    grouped: dict[str, tuple[Decimal | None, Decimal]] = {}
    distance_limit = _decimal(cfg.events.hot_liquidation_distance_ratio)
    notional_limit = _decimal(cfg.events.hot_position_notional_usd)
    for row in rows:
        address = normalize_address(row["address"])
        notional = abs(_decimal(row.get("notional_usd") or 0))
        distance = _distance(mids.get(str(row["coin"])), _optional_decimal(row.get("liquidation_px")))
        old_distance, old_notional = grouped.get(address, (None, Decimal(0)))
        nearest = distance if old_distance is None else old_distance if distance is None else min(old_distance, distance)
        grouped[address] = nearest, max(old_notional, notional)
    hot = [
        HotWallet(address, distance, notional)
        for address, (distance, notional) in grouped.items()
        if (distance is not None and distance <= distance_limit) or notional >= notional_limit
    ]
    hot.sort(
        key=lambda item: (
            item.liquidation_distance if item.liquidation_distance is not None else Decimal("Infinity"),
            -item.max_notional_usd,
            item.address,
        )
    )
    return hot[: cfg.tiers.hot_limit]


def combined_account_value(abstraction: Any, clearing: Any, spot: Any) -> Decimal:
    """按账户类型计算合约权益与现货 USDC 的合计权益。"""
    if not isinstance(clearing, Mapping):
        raise ValueError("clearinghouseState 响应必须是对象")
    summary = clearing.get("marginSummary")
    if not isinstance(summary, Mapping):
        raise ValueError("clearinghouseState.marginSummary 必须是对象")
    value = _decimal(summary.get("accountValue") or 0)
    account_type = abstraction
    if isinstance(abstraction, Mapping):
        account_type = abstraction.get("type") or abstraction.get("userAbstraction")
    if account_type not in {"unifiedAccount", "portfolioMargin"}:
        return value
    if not isinstance(spot, Mapping):
        return value
    balances = spot.get("balances")
    if not isinstance(balances, list):
        return value
    for balance in balances:
        if isinstance(balance, Mapping) and balance.get("coin") == "USDC":
            return value + _decimal(balance.get("total") or 0)
    return value


class HyperliquidMonitor:
    """把 REST/WS 状态转换为持久化快照、成交和一期事件。"""

    def __init__(
        self,
        cfg: Any,
        *,
        engine: AsyncEngine,
        rest: Any,
        sink: Any,
        clock: Clock = utcnow,
        fill_lookup_timeout_s: float = 5.0,
        account_lookup_timeout_s: float = 2.0,
    ) -> None:
        if fill_lookup_timeout_s <= 0 or account_lookup_timeout_s <= 0:
            raise ValueError("成交补查与账户查询超时必须大于 0")
        self.cfg = cfg
        self.engine = engine
        self.rest = rest
        self.sink = sink
        self.clock = clock
        self.fill_lookup_timeout_s = fill_lookup_timeout_s
        self.account_lookup_timeout_s = account_lookup_timeout_s
        self.mids: dict[str, Decimal] = {}
        self._account_types: dict[str, Any] = {}
        self._spot_states: dict[str, Mapping[str, Any]] = {}
        self._recent_fills: dict[tuple[str, str], Mapping[str, Any]] = {}
        self._deferred_fills: dict[str, tuple[datetime, set[str]]] = {}
        self._pending_reconciles: dict[str, _ReconcileProgress] = {}

    @property
    def deferred_fill_count(self) -> int:
        return len(self._deferred_fills)

    async def reconcile(self, address: str) -> bool:
        """尝试完成一次钱包 REST 对账；预算不足返回 False 供队列顺延。"""
        normalized = normalize_address(address)
        progress = self._pending_reconciles.get(normalized)
        if progress is None:
            async with self.engine.connect() as conn:
                position_dexes = set(
                    (
                        await conn.execute(
                            select(hl_positions.c.dex)
                            .where(hl_positions.c.address == normalized, hl_positions.c.dex != "")
                            .distinct()
                        )
                    ).scalars()
                )
                dex_cursor = await conn.scalar(
                    select(hl_state.c.cursor).where(hl_state.c.name == f"dexes:{normalized}")
                )
            remembered_dexes = (
                {str(dex) for dex in dex_cursor.get("dexes", []) if dex}
                if isinstance(dex_cursor, Mapping)
                else set()
            )
            progress = _ReconcileProgress(
                observed_at=self.clock(),
                dexes=(
                    "",
                    *sorted(position_dexes | remembered_dexes | set(self.cfg.hip3_dexes)),
                ),
                states={},
                abstraction=self._account_types.get(normalized, _MISSING),
            )
            self._pending_reconciles[normalized] = progress

        for dex in progress.dexes:
            if dex in progress.states:
                continue
            params = {"user": normalized, **({"dex": dex} if dex else {})}
            available, state = await self._try_info("clearinghouseState", **params)
            if not available:
                return False
            progress.states[dex] = state

        if progress.abstraction is _MISSING:
            available, abstraction = await self._try_info("userAbstraction", user=normalized)
            if not available:
                return False
            progress.abstraction = abstraction
            self._account_types[normalized] = abstraction
        if _is_unified(progress.abstraction) and progress.spot is _MISSING:
            available, spot = await self._try_info("spotClearinghouseState", user=normalized)
            if not available:
                return False
            progress.spot = spot

        clearing = progress.states[""]
        spot = None if progress.spot is _MISSING else progress.spot
        account_value = combined_account_value(progress.abstraction, clearing, spot)
        await self._process_states(
            normalized,
            [(dex, progress.states[dex]) for dex in progress.dexes],
            account_value=account_value,
            allow_fill_lookup=True,
            observed_at=progress.observed_at,
        )
        self._pending_reconciles.pop(normalized, None)
        return True

    async def lookup_fill_evidence(
        self,
        address: str,
        changed_coins: set[str],
        *,
        remember: bool = True,
    ) -> Mapping[str, Any] | None:
        """按一分钟保护补查成交，成功提交成交与游标后返回本次最佳证据。"""
        normalized = normalize_address(address)
        state_name = f"fills:{normalized}"
        async with self.engine.connect() as conn:
            cursor = await conn.scalar(select(hl_state.c.cursor).where(hl_state.c.name == state_name))
        cursor = dict(cursor) if isinstance(cursor, Mapping) else {}
        last_query = _parse_datetime(cursor.get("last_query_at"))
        now = self.clock()
        if last_query is not None:
            elapsed = (now - last_query).total_seconds()
            if elapsed < self.cfg.frequencies.fill_query_min_interval_s:
                await self._defer_fill_query(
                    normalized,
                    changed_coins,
                    last_query
                    + timedelta(seconds=self.cfg.frequencies.fill_query_min_interval_s),
                )
                return None

        start_time = int(cursor.get("last_time") or 0)
        available, raw_fills = await self._try_info(
            "userFillsByTime",
            user=normalized,
            startTime=start_time,
        )
        if not available:
            await self._defer_fill_query(normalized, changed_coins, now + timedelta(seconds=1))
            return None
        fills = _unwrap_fills(raw_fills, is_twap=False)
        await self._store_fills(normalized, fills, last_query_at=now)
        matching = [fill for fill, _ in fills if fill.get("coin") in changed_coins]
        if not matching:
            available, raw_twap = await self._try_info("userTwapSliceFills", user=normalized)
            if not available:
                await self._defer_fill_query(
                    normalized,
                    changed_coins,
                    now + timedelta(seconds=self.cfg.frequencies.fill_query_min_interval_s),
                )
                return None
            twap_fills = _unwrap_fills(raw_twap, is_twap=True)
            await self._store_fills(normalized, twap_fills, last_query_at=now)
            fills.extend(twap_fills)

        matching = [fill for fill, _ in fills if fill.get("coin") in changed_coins]
        await self._clear_deferred_fill(normalized)
        if remember:
            for evidence in matching:
                coin = evidence.get("coin")
                if isinstance(coin, str):
                    self._recent_fills[(normalized, coin)] = evidence
        return max(matching, key=lambda item: int(item.get("time") or 0), default=None)

    async def run_due_fill_queries(self) -> None:
        """执行到期的合并补查；失败或预算不足时保留任务稍后重试。"""
        await self._restore_deferred_fills()
        now = self.clock()
        due = [
            (address, coins)
            for address, (due_at, coins) in self._deferred_fills.items()
            if due_at <= now
        ]
        for address, coins in due:
            self._deferred_fills.pop(address, None)
            try:
                await asyncio.wait_for(
                    self.lookup_fill_evidence(address, set(coins), remember=False),
                    timeout=self.fill_lookup_timeout_s,
                )
            except asyncio.TimeoutError:
                await self._defer_fill_query(
                    address,
                    coins,
                    self.clock() + timedelta(seconds=1),
                )
                logger.warning("钱包 %s…%s 延期成交补查超时，将继续顺延", address[:6], address[-4:])

    async def _defer_fill_query(
        self,
        address: str,
        coins: Iterable[str],
        due_at: datetime,
    ) -> None:
        existing = self._deferred_fills.get(address)
        merged = set(coins)
        if existing is not None:
            due_at = max(due_at, existing[0])
            merged.update(existing[1])
        self._deferred_fills[address] = (due_at, merged)
        name = f"fills:{address}"
        async with self.engine.begin() as conn:
            old_cursor = await conn.scalar(select(hl_state.c.cursor).where(hl_state.c.name == name))
            cursor = dict(old_cursor) if isinstance(old_cursor, Mapping) else {}
            cursor["pending_coins"] = sorted(merged)
            cursor["next_query_at"] = due_at.isoformat()
            stmt = pg_insert(hl_state).values(name=name, cursor=cursor, updated_at=self.clock())
            await conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["name"],
                    set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
                )
            )

    async def _clear_deferred_fill(self, address: str) -> None:
        self._deferred_fills.pop(address, None)
        name = f"fills:{address}"
        async with self.engine.begin() as conn:
            old_cursor = await conn.scalar(select(hl_state.c.cursor).where(hl_state.c.name == name))
            if not isinstance(old_cursor, Mapping):
                return
            cursor = dict(old_cursor)
            had_pending = "pending_coins" in cursor or "next_query_at" in cursor
            cursor.pop("pending_coins", None)
            cursor.pop("next_query_at", None)
            if not had_pending:
                return
            stmt = pg_insert(hl_state).values(name=name, cursor=cursor, updated_at=self.clock())
            await conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["name"],
                    set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
                )
            )

    async def _restore_deferred_fills(self) -> None:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(hl_state.c.name, hl_state.c.cursor).where(hl_state.c.name.like("fills:%"))
                )
            ).all()
        for name, cursor in rows:
            if not isinstance(cursor, Mapping):
                continue
            due_at = _parse_datetime(cursor.get("next_query_at"))
            coins = cursor.get("pending_coins")
            if due_at is None or not isinstance(coins, list):
                continue
            address = normalize_address(name.removeprefix("fills:"))
            existing = self._deferred_fills.get(address)
            merged = {str(coin) for coin in coins if coin}
            if existing is not None:
                due_at = max(due_at, existing[0])
                merged.update(existing[1])
            self._deferred_fills[address] = (due_at, merged)

    async def handle_core_message(self, message: Any) -> None:
        """处理核心连接上的成交、状态和 allMids 消息。"""
        if not isinstance(message, Mapping):
            return
        channel = message.get("channel")
        data = message.get("data")
        if channel == "allMids":
            mids = data.get("mids") if isinstance(data, Mapping) and isinstance(data.get("mids"), Mapping) else data
            if isinstance(mids, Mapping):
                await self.update_mids(mids)
            return
        if not isinstance(data, Mapping):
            return
        address = data.get("user")
        if not isinstance(address, str):
            return
        normalized = normalize_address(address)
        if channel == "userFills":
            fills = _unwrap_fills(data.get("fills", []), is_twap=False)
            await self._store_fills(normalized, fills)
            if data.get("isSnapshot"):
                return
            for fill, _ in fills:
                coin = fill.get("coin")
                if isinstance(coin, str):
                    self._recent_fills[(normalized, coin)] = fill
            await self._emit_liquidations(normalized, [fill for fill, _ in fills])
            return
        if channel == "allDexsClearinghouseState":
            states = _all_dex_states(data)
            if not states:
                return
            account_value = await self._core_account_value(normalized, states[0][1])
            await self._process_states(
                normalized,
                states,
                account_value=account_value,
                allow_fill_lookup=False,
            )

    async def update_mids(self, mids: Mapping[str, Any]) -> None:
        """更新本地标记价，并只用本地数据检查热点仓位强平风险。"""
        changed: set[str] = set()
        for coin, value in mids.items():
            try:
                price = _decimal(value)
            except ValueError:
                continue
            if price <= 0:
                continue
            symbol = str(coin)
            self.mids[symbol] = price
            changed.add(symbol)
        if not changed:
            return
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(hl_positions, hl_wallets.c.tier)
                    .join(hl_wallets, hl_wallets.c.address == hl_positions.c.address)
                    .where(
                        hl_wallets.c.tier == "focus",
                        hl_wallets.c.excluded.is_(False),
                    )
                )
            ).mappings().all()
        selected = {item.address for item in select_hot_wallets(rows, self.mids, self.cfg)}
        for row in rows:
            if row["address"] not in selected or row["coin"] not in changed:
                continue
            snapshot = _snapshot_from_row(row, updated_at=self.clock())
            async with self.engine.connect() as conn:
                risk_state = await load_liquidation_risk_state(
                    conn, snapshot.address, snapshot.dex, snapshot.coin
                )
            result = detect_events_with_state(
                snapshot,
                snapshot,
                cfg=self.cfg,
                mark_px=self.mids.get(snapshot.coin),
                liquidation_risk_state=risk_state,
                wallet={"tier": row["tier"]},
            )
            if result.events and self.sink is not None:
                await self.sink.ingest(FetchResult(result.events, None), collector="hl_monitor")
            async with self.engine.begin() as conn:
                await save_liquidation_risk_state(
                    conn,
                    snapshot.address,
                    snapshot.dex,
                    snapshot.coin,
                    result.liquidation_risk_state,
                    self.clock(),
                )

    async def hot_wallets(self) -> list[HotWallet]:
        """读取重点钱包当前仓位并计算热点集合。"""
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(hl_positions)
                    .join(hl_wallets, hl_wallets.c.address == hl_positions.c.address)
                    .where(hl_wallets.c.tier == "focus", hl_wallets.c.excluded.is_(False))
                )
            ).mappings().all()
        return select_hot_wallets(rows, self.mids, self.cfg)

    async def _process_states(
        self,
        address: str,
        states: list[tuple[str, Any]],
        *,
        account_value: Decimal,
        allow_fill_lookup: bool,
        observed_at: datetime | None = None,
    ) -> None:
        now = observed_at or self.clock()
        total_notional = sum(
            (sum((snapshot.notional_usd for snapshot in _snapshots(state, address, dex, now)), Decimal(0)) for dex, state in states),
            Decimal(0),
        )
        transitions: list[tuple[PositionSnapshot, PositionSnapshot]] = []
        responses: list[tuple[str, Any]] = []
        for dex, state in states:
            current = {snapshot.coin: snapshot for snapshot in _snapshots(state, address, dex, now)}
            async with self.engine.connect() as conn:
                old_rows = (
                    await conn.execute(
                        select(hl_positions).where(
                            hl_positions.c.address == address,
                            hl_positions.c.dex == dex,
                        )
                    )
                ).mappings().all()
            previous = {row["coin"]: _snapshot_from_row(row, updated_at=row["updated_at"]) for row in old_rows}
            for coin in sorted(previous.keys() | current.keys()):
                old = previous.get(coin) or _zero_snapshot(address, dex, coin, now)
                new = current.get(coin) or _zero_snapshot(address, dex, coin, now)
                old = _with_account(old, account_value, total_notional)
                new = _with_account(new, account_value, total_notional)
                transitions.append((old, new))
            responses.append((dex, state))

        changed_coins = {new.coin for old, new in transitions if old.szi != new.szi}
        fill = None
        if changed_coins and allow_fill_lookup:
            try:
                fill = await asyncio.wait_for(
                    self.lookup_fill_evidence(address, changed_coins),
                    timeout=self.fill_lookup_timeout_s,
                )
            except asyncio.TimeoutError:
                await self._defer_fill_query(
                    address,
                    changed_coins,
                    self.clock() + timedelta(seconds=1),
                )
                logger.warning(
                    "钱包 %s…%s 成交补查超过 %g 秒，先按快照生成事件",
                    address[:6],
                    address[-4:],
                    self.fill_lookup_timeout_s,
                )

        async with self.engine.connect() as conn:
            wallet_row = (
                await conn.execute(select(hl_wallets).where(hl_wallets.c.address == address))
            ).mappings().first()
            risk_states = {
                (new.dex, new.coin): await load_liquidation_risk_state(conn, address, new.dex, new.coin)
                for _, new in transitions
            }
        wallet = dict(wallet_row or {})
        events = []
        next_risks: dict[tuple[str, str], str | None] = {}
        used_fill_keys: list[tuple[str, str]] = []
        for old, new in transitions:
            if fill is not None and fill.get("coin") == new.coin:
                evidence = fill
            else:
                evidence = self._recent_fills.get((address, new.coin))
            if evidence is not None:
                used_fill_keys.append((address, new.coin))
            result = detect_events_with_state(
                old,
                new,
                cfg=self.cfg,
                fill=evidence,
                mark_px=self.mids.get(new.coin),
                liquidation_risk_state=risk_states[(new.dex, new.coin)],
                wallet=wallet,
            )
            events.extend(result.events)
            next_risks[(new.dex, new.coin)] = result.liquidation_risk_state

        if events and self.sink is not None:
            await self.sink.ingest(FetchResult(events, None), collector="hl_monitor")
        async with self.engine.begin() as conn:
            for dex, state in responses:
                await upsert_position_snapshot(
                    conn,
                    address=address,
                    dex=dex,
                    response=state,
                    updated_at=now,
                )
            for (dex, coin), level in next_risks.items():
                await save_liquidation_risk_state(conn, address, dex, coin, level, now)
            dexes = sorted({dex for dex, _ in responses if dex})
            if dexes:
                await _save_known_dexes(conn, address, dexes, now)
        for key in used_fill_keys:
            self._recent_fills.pop(key, None)

    async def _emit_liquidations(self, address: str, fills: list[Mapping[str, Any]]) -> None:
        if self.sink is None:
            return
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(select(hl_positions).where(hl_positions.c.address == address))
            ).mappings().all()
        events = []
        for fill in fills:
            row = next((item for item in rows if item["coin"] == fill.get("coin")), None)
            if row is None:
                continue
            snapshot = _snapshot_from_row(row, updated_at=self.clock())
            result = detect_events_with_state(
                snapshot,
                snapshot,
                cfg=self.cfg,
                fill=fill,
                mark_px=self.mids.get(snapshot.coin),
            )
            events.extend(event for event in result.events if event.event_type == "hl_liquidated")
        if events:
            await self.sink.ingest(FetchResult(events, None), collector="hl_monitor")

    async def _store_fills(
        self,
        address: str,
        fills: list[tuple[Mapping[str, Any], bool]],
        *,
        last_query_at: datetime | None = None,
    ) -> None:
        state_name = f"fills:{address}"
        async with self.engine.begin() as conn:
            old_cursor = await conn.scalar(select(hl_state.c.cursor).where(hl_state.c.name == state_name))
            cursor = dict(old_cursor) if isinstance(old_cursor, Mapping) else {}
            last_time = int(cursor.get("last_time") or 0)
            for fill, is_twap in fills:
                row = _fill_row(address, fill, is_twap=is_twap)
                stmt = pg_insert(hl_fills).values(**row).on_conflict_do_nothing(
                    index_elements=["address", "tid"]
                )
                await conn.execute(stmt)
                last_time = max(last_time, int(fill.get("time") or 0))
            cursor["last_time"] = last_time
            if last_query_at is not None:
                cursor["last_query_at"] = last_query_at.isoformat()
            stmt = pg_insert(hl_state).values(name=state_name, cursor=cursor, updated_at=self.clock())
            await conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["name"],
                    set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
                )
            )

    async def _try_info(self, request_type: str, **params: Any) -> tuple[bool, Any]:
        if self.rest is None:
            return False, None
        method = getattr(self.rest, "try_info", None)
        if method is not None:
            return await method(request_type, budget="monitor", **params)
        return True, await self.rest.info(request_type, budget="monitor", **params)

    async def _core_account_value(self, address: str, clearing: Any) -> Decimal:
        """有界刷新核心钱包账户类型与现货 USDC，失败时使用最近缓存。"""
        abstraction = self._account_types.get(address)
        if abstraction is None:
            try:
                available, value = await asyncio.wait_for(
                    self._try_info("userAbstraction", user=address),
                    timeout=self.account_lookup_timeout_s,
                )
            except Exception as exc:
                logger.warning(
                    "钱包 %s…%s 账户类型查询失败，暂按普通账户处理：%s",
                    address[:6],
                    address[-4:],
                    exc,
                )
            else:
                if available:
                    abstraction = value
                    self._account_types[address] = value
        abstraction = abstraction or "disabled"
        spot = self._spot_states.get(address)
        if _is_unified(abstraction):
            try:
                available, value = await asyncio.wait_for(
                    self._try_info("spotClearinghouseState", user=address),
                    timeout=self.account_lookup_timeout_s,
                )
            except Exception as exc:
                logger.warning(
                    "钱包 %s…%s 现货 USDC 查询失败，使用最近缓存：%s",
                    address[:6],
                    address[-4:],
                    exc,
                )
            else:
                if available and isinstance(value, Mapping):
                    spot = value
                    self._spot_states[address] = value
        return combined_account_value(abstraction, clearing, spot)


async def _save_known_dexes(
    conn: AsyncConnection,
    address: str,
    dexes: Iterable[str],
    updated_at: datetime,
) -> None:
    """持久化钱包曾出现过的 HIP-3 dex，避免平仓后停止对账。"""
    name = f"dexes:{normalize_address(address)}"
    old_cursor = await conn.scalar(select(hl_state.c.cursor).where(hl_state.c.name == name))
    remembered = (
        {str(dex) for dex in old_cursor.get("dexes", []) if dex}
        if isinstance(old_cursor, Mapping)
        else set()
    )
    remembered.update(str(dex) for dex in dexes if dex)
    cursor = {"dexes": sorted(remembered)}
    stmt = pg_insert(hl_state).values(name=name, cursor=cursor, updated_at=updated_at)
    await conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
        )
    )


def _snapshots(response: Any, address: str, dex: str, updated_at: datetime) -> list[PositionSnapshot]:
    if not isinstance(response, Mapping):
        raise ValueError("clearinghouseState 响应必须是对象")
    items = response.get("assetPositions")
    if not isinstance(items, list):
        raise ValueError("clearinghouseState.assetPositions 必须是数组")
    snapshots: list[PositionSnapshot] = []
    for item in items:
        position = item.get("position") if isinstance(item, Mapping) else None
        if not isinstance(position, Mapping):
            raise ValueError("clearinghouseState.position 必须是对象")
        leverage = position.get("leverage")
        leverage_value = leverage.get("value") if isinstance(leverage, Mapping) else leverage
        snapshots.append(
            PositionSnapshot(
                address=address,
                dex=dex,
                coin=str(position["coin"]),
                szi=_decimal(position.get("szi") or 0),
                notional_usd=abs(_decimal(position.get("positionValue") or 0)),
                updated_at=updated_at,
                entry_px=_optional_decimal(position.get("entryPx")),
                liquidation_px=_optional_decimal(position.get("liquidationPx")),
                leverage=_optional_decimal(leverage_value),
                unrealized_pnl=_optional_decimal(position.get("unrealizedPnl")),
            )
        )
    return snapshots


def _snapshot_from_row(row: Mapping[str, Any], *, updated_at: datetime) -> PositionSnapshot:
    return PositionSnapshot(
        address=row["address"],
        dex=row["dex"],
        coin=row["coin"],
        szi=_decimal(row["szi"]),
        notional_usd=abs(_decimal(row.get("notional_usd") or 0)),
        updated_at=updated_at,
        entry_px=_optional_decimal(row.get("entry_px")),
        liquidation_px=_optional_decimal(row.get("liquidation_px")),
        leverage=_optional_decimal(row.get("leverage")),
        unrealized_pnl=_optional_decimal(row.get("unrealized_pnl")),
    )


def _zero_snapshot(address: str, dex: str, coin: str, updated_at: datetime) -> PositionSnapshot:
    return PositionSnapshot(address, dex, coin, Decimal(0), Decimal(0), updated_at)


def _with_account(snapshot: PositionSnapshot, value: Decimal, notional: Decimal) -> PositionSnapshot:
    return PositionSnapshot(
        **{
            **snapshot.__dict__,
            "account_value": value,
            "account_notional_usd": notional,
        }
    )


def _all_dex_states(data: Mapping[str, Any]) -> list[tuple[str, Any]]:
    raw = data.get("clearinghouseStates") or data.get("states")
    if raw is None and isinstance(data.get("assetPositions"), list):
        return [("", data)]
    states: list[tuple[str, Any]] = []
    if isinstance(raw, Mapping):
        states.extend((str(dex), state) for dex, state in raw.items())
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                states.append((str(item[0]), item[1]))
            elif isinstance(item, Mapping):
                if isinstance(item.get("assetPositions"), list):
                    states.append(("", item))
                else:
                    dex = item.get("dex") or ""
                    state = item.get("clearinghouseState") or item.get("state")
                    if state is not None:
                        states.append((str(dex), state))
    states.sort(key=lambda item: (item[0] != "", item[0]))
    return states


def _unwrap_fills(raw: Any, *, is_twap: bool) -> list[tuple[Mapping[str, Any], bool]]:
    if not isinstance(raw, list):
        raise ValueError("成交响应必须是数组")
    result: list[tuple[Mapping[str, Any], bool]] = []
    for item in raw:
        fill = item.get("fill") if is_twap and isinstance(item, Mapping) else item
        if not isinstance(fill, Mapping):
            raise ValueError("成交记录必须是对象")
        result.append((fill, is_twap))
    return result


def _fill_row(address: str, fill: Mapping[str, Any], *, is_twap: bool) -> dict[str, Any]:
    try:
        occurred_at = datetime.fromtimestamp(int(fill["time"]) / 1000, tz=timezone.utc)
        tid = int(fill["tid"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("成交记录缺少有效 time 或 tid") from exc
    return {
        "address": normalize_address(address),
        "tid": tid,
        "coin": str(fill["coin"]),
        "dir": str(fill.get("dir") or ""),
        "px": _decimal(fill.get("px") or 0),
        "sz": _decimal(fill.get("sz") or 0),
        "closed_pnl": _decimal(fill.get("closedPnl") or 0),
        "fee": _decimal(fill.get("fee") or 0),
        "crossed": bool(fill.get("crossed")),
        "occurred_at": occurred_at,
        "hash": str(fill.get("hash") or ""),
        "is_twap": is_twap,
        "liquidation": dict(fill["liquidation"]) if isinstance(fill.get("liquidation"), Mapping) else None,
    }


def _is_unified(value: Any) -> bool:
    if isinstance(value, Mapping):
        value = value.get("type") or value.get("userAbstraction")
    return value in {"unifiedAccount", "portfolioMargin"}


def _distance(mark: Decimal | None, liquidation: Decimal | None) -> Decimal | None:
    if mark is None or liquidation is None or mark <= 0:
        return None
    return abs(mark - liquidation) / mark


def _decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"数值无效：{value}") from exc
    if not parsed.is_finite():
        raise ValueError(f"数值无效：{value}")
    return parsed


def _optional_decimal(value: Any) -> Decimal | None:
    return None if value is None else _decimal(value)


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        return None
    return result if result.tzinfo is not None else None
