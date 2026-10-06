"""Hyperliquid 巨鲸运行时监控：优先对账、快照、成交与实时价格。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum, IntEnum
from typing import Any

from sqlalchemy import String, and_, cast, delete, func, literal, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.hyperliquid.baseline import (
    load_last_observed_at,
    load_monitor_baseline,
    save_last_observed_at,
    save_monitor_baseline,
)
from tidewatch.hyperliquid.detect import (
    PositionSnapshot,
    delete_liquidation_risk_state,
    detect_events_with_state,
    detect_liquidation_event,
    load_liquidation_risk_state,
    save_liquidation_risk_state,
)
from tidewatch.hyperliquid.positions import upsert_position_snapshot
from tidewatch.hyperliquid.logging import exception_summary
from tidewatch.hyperliquid.registry import normalize_address
from tidewatch.models import FetchResult
from tidewatch.store.tables import (
    events as event_rows,
    hl_fills,
    hl_positions,
    hl_state,
    hl_wallets,
)


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
    deadline_at: float | None = None
    aging_since: float | None = None


class PriorityReconcileQueue:
    """按钱包去重的对账队列；预算不足时任务可原样顺延。"""

    def __init__(
        self,
        *,
        hot_aging_s: float = 30,
        normal_aging_s: float = 120,
    ) -> None:
        if hot_aging_s <= 0 or normal_aging_s <= 0:
            raise ValueError("对账任务 aging 时间必须大于 0")
        self._tasks: dict[str, ScheduledReconcile] = {}
        self.hot_aging_s = hot_aging_s
        self.normal_aging_s = normal_aging_s

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
        deadline_at: float | None = None,
    ) -> None:
        normalized = normalize_address(address)
        if deadline_at is not None:
            due_at = min(due_at, deadline_at)
        incoming = ScheduledReconcile(
            normalized,
            priority,
            due_at,
            reason,
            deadline_at,
            due_at,
        )
        current = self._tasks.get(normalized)
        if current is None:
            self._tasks[normalized] = incoming
            return
        if priority < current.priority:
            self._tasks[normalized] = incoming
            return
        if priority == current.priority:
            if priority is ReconcilePriority.TRADE:
                first_deadline = current.deadline_at or incoming.deadline_at
                capped_due = (
                    min(incoming.due_at, first_deadline)
                    if first_deadline is not None
                    else incoming.due_at
                )
                self._tasks[normalized] = ScheduledReconcile(
                    normalized,
                    priority,
                    capped_due,
                    reason,
                    first_deadline,
                    current.aging_since,
                )
            elif due_at < current.due_at:
                self._tasks[normalized] = incoming

    def pop_ready(
        self,
        now: float,
        *,
        excluded: set[str] | None = None,
    ) -> ScheduledReconcile | None:
        blocked = excluded or set()
        ready = [
            task
            for task in self._tasks.values()
            if task.due_at <= now and task.address not in blocked
        ]
        if not ready:
            return None
        task = min(ready, key=lambda item: self._sort_key(item, now))
        del self._tasks[task.address]
        return task

    def defer(self, task: ScheduledReconcile, *, due_at: float) -> None:
        current = self._tasks.get(task.address)
        if current is not None and current.priority <= task.priority:
            return
        self._tasks[task.address] = ScheduledReconcile(
            task.address,
            task.priority,
            due_at,
            task.reason,
            aging_since=(
                task.aging_since
                if task.aging_since is not None
                else task.due_at
            ),
        )

    def discard(self, address: str) -> None:
        """丢弃指定钱包尚未执行的对账任务。"""
        self._tasks.pop(normalize_address(address), None)

    def addresses(self) -> set[str]:
        """返回当前排队钱包快照，供名单生命周期差分清理。"""
        return set(self._tasks)

    def _sort_key(
        self,
        task: ScheduledReconcile,
        now: float,
    ) -> tuple[int, float, str]:
        overdue = now - (
            task.aging_since if task.aging_since is not None else task.due_at
        )
        aged = (
            task.priority is ReconcilePriority.NORMAL
            and overdue >= self.normal_aging_s
        ) or (
            task.priority in {ReconcilePriority.LIQUIDATION, ReconcilePriority.HOT}
            and overdue >= self.hot_aging_s
        )
        effective_priority = ReconcilePriority.TRADE if aged else task.priority
        return int(effective_priority), task.due_at, task.address


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


class _FillLookupStatus(Enum):
    COMPLETE = "complete"
    THROTTLED = "throttled"
    DEFERRED = "deferred"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class _FillLookupResult:
    evidence: Mapping[str, Any] | None
    status: _FillLookupStatus


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
        deferred_fill_lookup_timeout_s: float | None = None,
        account_lookup_timeout_s: float = 5.0,
        on_position: Callable[[str, Mapping[str, Decimal]], Awaitable[None]] | None = None,
        on_market: Callable[[Mapping[str, Decimal]], Awaitable[None]] | None = None,
    ) -> None:
        deferred_timeout = (
            cfg.frequencies.deferred_fill_lookup_timeout_s
            if deferred_fill_lookup_timeout_s is None
            else deferred_fill_lookup_timeout_s
        )
        if (
            fill_lookup_timeout_s <= 0
            or deferred_timeout <= 0
            or account_lookup_timeout_s <= 0
        ):
            raise ValueError("成交补查与账户查询超时必须大于 0")
        self.cfg = cfg
        self.engine = engine
        self.rest = rest
        self.sink = sink
        self.clock = clock
        self.fill_lookup_timeout_s = fill_lookup_timeout_s
        self.deferred_fill_lookup_timeout_s = deferred_timeout
        self.account_lookup_timeout_s = account_lookup_timeout_s
        self.on_position = on_position
        self.on_market = on_market
        self.mids: dict[str, Decimal] = {}
        self._account_metadata: dict[str, dict[str, Any]] = {}
        self._recent_fills: dict[tuple[str, str], Mapping[str, Any]] = {}
        self._deferred_fills: dict[str, tuple[datetime, set[str]]] = {}
        self._pending_reconciles: dict[str, _ReconcileProgress] = {}
        self._liquidation_locks: dict[str, asyncio.Lock] = {}
        self._fill_timeout_warnings: dict[str, datetime] = {}
        self._account_lookup_warnings: dict[tuple[str, str], datetime] = {}

    @property
    def deferred_fill_count(self) -> int:
        return len(self._deferred_fills)

    async def reconcile(self, address: str) -> bool:
        """只用仓位状态完成关键路径对账；账户元数据由后台低优先级刷新。"""
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
                metadata_cursor = await conn.scalar(
                    select(hl_state.c.cursor).where(
                        hl_state.c.name == f"account_metadata:{normalized}"
                    )
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
                    *sorted(position_dexes | remembered_dexes),
                ),
                states={},
            )
            self._pending_reconciles[normalized] = progress
            if isinstance(metadata_cursor, Mapping):
                self._account_metadata[normalized] = dict(metadata_cursor)

        pending: list[tuple[str, str | None, Any]] = []
        for dex in progress.dexes:
            if dex not in progress.states:
                params = {"user": normalized, **({"dex": dex} if dex else {})}
                pending.append(
                    ("state", dex, self._try_info("clearinghouseState", **params))
                )
        if pending:
            results = await asyncio.gather(*(request for _, _, request in pending))
            complete = True
            for (kind, dex, _), (available, value) in zip(pending, results, strict=True):
                if not available:
                    complete = False
                elif kind == "state":
                    assert dex is not None
                    progress.states[dex] = value
            if not complete:
                return False

        clearing = progress.states[""]
        account_value = self._account_value_from_metadata(normalized, clearing)
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
        result = await self._lookup_fill_evidence(address, changed_coins, remember=remember)
        return result.evidence

    async def _lookup_fill_evidence(
        self,
        address: str,
        changed_coins: set[str],
        *,
        remember: bool = True,
        timeout_s: float | None = None,
        recover_persisted_on_throttle: bool = False,
    ) -> _FillLookupResult:
        """补查成交并区分节流、预算顺延与真实网络超时。"""
        request_timeout_s = (
            self.fill_lookup_timeout_s if timeout_s is None else timeout_s
        )
        normalized = normalize_address(address)
        state_name = f"fills:{normalized}"
        async with self.engine.connect() as conn:
            cursor = await conn.scalar(select(hl_state.c.cursor).where(hl_state.c.name == state_name))
            baseline_at = await load_monitor_baseline(conn, normalized)
            last_observed_at = await load_last_observed_at(conn, normalized)
        if baseline_at is None:
            self._deferred_fills.pop(normalized, None)
            return _FillLookupResult(None, _FillLookupStatus.COMPLETE)
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
                logger.debug(
                    "钱包 %s…%s 成交补查节流中，%.1f 秒后可再次查询",
                    normalized[:6],
                    normalized[-4:],
                    self.cfg.frequencies.fill_query_min_interval_s - elapsed,
                )
                evidence = None
                if recover_persisted_on_throttle:
                    evidence = await self._load_persisted_fill_evidence(
                        normalized,
                        changed_coins,
                        since=last_observed_at or baseline_at,
                    )
                if remember and evidence is not None:
                    coin = evidence.get("coin")
                    if isinstance(coin, str):
                        self._recent_fills[(normalized, coin)] = evidence
                return _FillLookupResult(evidence, _FillLookupStatus.THROTTLED)

        baseline_ms = int(baseline_at.timestamp() * 1000)
        start_time = max(int(cursor.get("last_time") or 0), baseline_ms)
        try:
            available, raw_fills = await asyncio.wait_for(
                self._try_info(
                    "userFillsByTime",
                    user=normalized,
                    startTime=start_time,
                ),
                timeout=request_timeout_s,
            )
        except asyncio.TimeoutError:
            await self._defer_fill_query(normalized, changed_coins, now + timedelta(seconds=1))
            return _FillLookupResult(None, _FillLookupStatus.TIMEOUT)
        if not available:
            await self._defer_fill_query(normalized, changed_coins, now + timedelta(seconds=1))
            return _FillLookupResult(None, _FillLookupStatus.DEFERRED)
        fills = [
            item
            for item in _unwrap_fills(raw_fills, is_twap=False)
            if int(item[0].get("time") or 0) >= baseline_ms
        ]
        await self._store_fills(normalized, fills, last_query_at=now)
        matching = [fill for fill, _ in fills if fill.get("coin") in changed_coins]
        if not matching:
            try:
                available, raw_twap = await asyncio.wait_for(
                    self._try_info("userTwapSliceFills", user=normalized),
                    timeout=request_timeout_s,
                )
            except asyncio.TimeoutError:
                await self._defer_fill_query(
                    normalized,
                    changed_coins,
                    now + timedelta(seconds=1),
                )
                return _FillLookupResult(None, _FillLookupStatus.TIMEOUT)
            if not available:
                await self._defer_fill_query(
                    normalized,
                    changed_coins,
                    now + timedelta(seconds=self.cfg.frequencies.fill_query_min_interval_s),
                )
                return _FillLookupResult(None, _FillLookupStatus.DEFERRED)
            twap_fills = [
                item
                for item in _unwrap_fills(raw_twap, is_twap=True)
                if int(item[0].get("time") or 0) >= baseline_ms
            ]
            await self._store_fills(normalized, twap_fills, last_query_at=now)
            fills.extend(twap_fills)

        matching = [fill for fill, _ in fills if fill.get("coin") in changed_coins]
        await self._clear_deferred_fill(normalized)
        if remember:
            for evidence in matching:
                coin = evidence.get("coin")
                if isinstance(coin, str):
                    self._recent_fills[(normalized, coin)] = evidence
        evidence = max(matching, key=lambda item: int(item.get("time") or 0), default=None)
        return _FillLookupResult(evidence, _FillLookupStatus.COMPLETE)

    async def _load_persisted_fill_evidence(
        self,
        address: str,
        changed_coins: set[str],
        *,
        since: datetime,
    ) -> Mapping[str, Any] | None:
        """在补查节流时恢复上一快照之后已提交的最新成交证据。"""
        if not changed_coins:
            return None
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    select(hl_fills)
                    .where(
                        hl_fills.c.address == address,
                        hl_fills.c.coin.in_(changed_coins),
                        hl_fills.c.occurred_at >= since,
                    )
                    .order_by(hl_fills.c.occurred_at.desc(), hl_fills.c.tid.desc())
                    .limit(1)
                )
            ).mappings().first()
        return None if row is None else _fill_from_row(row)

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
            result = await self._lookup_fill_evidence(
                address,
                set(coins),
                remember=False,
                timeout_s=self.deferred_fill_lookup_timeout_s,
            )
            if result.status is _FillLookupStatus.TIMEOUT:
                self._log_fill_timeout(address, deferred=True)
            elif result.status is _FillLookupStatus.DEFERRED:
                logger.debug(
                    "钱包 %s…%s 延期成交补查因 REST 预算不足，将继续顺延",
                    address[:6],
                    address[-4:],
                )

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
            async with self.engine.connect() as conn:
                baseline_at = await load_monitor_baseline(conn, normalized)
            if baseline_at is None:
                return
            baseline_ms = int(baseline_at.timestamp() * 1000)
            current_fills = [
                fill for fill, _ in fills if int(fill.get("time") or 0) >= baseline_ms
            ]
            for fill in current_fills:
                coin = fill.get("coin")
                if isinstance(coin, str):
                    self._recent_fills[(normalized, coin)] = fill
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
            baseline_names = set(
                (
                    await conn.execute(
                        select(hl_state.c.name).where(
                            hl_state.c.name.like("monitor_baseline:%")
                        )
                    )
                ).scalars()
            )
        selected = {item.address for item in select_hot_wallets(rows, self.mids, self.cfg)}
        for row in rows:
            if (
                row["address"] not in selected
                or row["coin"] not in changed
                or f"monitor_baseline:{row['address']}" not in baseline_names
            ):
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
            stable_suffix = str(int(row["updated_at"].timestamp() * 1000))
            for event in result.events:
                if event.event_type != "hl_liq_risk":
                    continue
                level = event.raw.get("liquidation_risk_level")
                event.dedupe_key = (
                    f"{event.dedupe_key.rsplit(':', 1)[0]}:"
                    f"{stable_suffix}:{level}"
                )
            if result.events and self.sink is not None:
                await self.sink.ingest(
                    FetchResult(result.events, None),
                    collector="hl_monitor",
                    track_collector_state=False,
                )
            async with self.engine.begin() as conn:
                await save_liquidation_risk_state(
                    conn,
                    snapshot.address,
                    snapshot.dex,
                    snapshot.coin,
                    result.liquidation_risk_state,
                    self.clock(),
                )
        if self.on_market is not None:
            try:
                await self.on_market(dict(self.mids))
            except Exception as exc:
                logger.error("广播 Hyperliquid 行情更新失败：%s", exception_summary(exc))

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

    async def cleanup_liquidation_risk_states(self) -> int:
        """删除无当前受监控仓位对应关系的强平风险状态。"""
        async with self.engine.begin() as conn:
            positions = (
                await conn.execute(
                    select(
                        hl_positions.c.address,
                        hl_positions.c.dex,
                        hl_positions.c.coin,
                    )
                    .join(
                        hl_wallets,
                        hl_wallets.c.address == hl_positions.c.address,
                    )
                    .where(
                        hl_wallets.c.tier.in_(("focus", "core")),
                        hl_wallets.c.excluded.is_(False),
                    )
                )
            ).all()
            valid_names = {
                f"liq_risk:{address}:{dex}:{coin}"
                for address, dex, coin in positions
            }
            existing_names = set(
                (
                    await conn.execute(
                        select(hl_state.c.name).where(
                            hl_state.c.name.like("liq_risk:%")
                        )
                    )
                ).scalars()
            )
            stale_names = existing_names - valid_names
            if not stale_names:
                return 0
            result = await conn.execute(
                delete(hl_state).where(hl_state.c.name.in_(stale_names))
            )
        return result.rowcount or 0

    async def cleanup_old_fills(self) -> int:
        """清理画像窗口之外的过期成交；配置保留期至少为 30 天。"""
        cutoff = self.clock() - timedelta(days=self.cfg.fill_retention_days)
        async with self.engine.begin() as conn:
            result = await conn.execute(delete(hl_fills).where(hl_fills.c.occurred_at < cutoff))
        return result.rowcount or 0

    def forget_wallet(self, address: str) -> None:
        """清除已退出监控钱包的全部进程内状态。"""
        normalized = normalize_address(address)
        self._account_metadata.pop(normalized, None)
        self._deferred_fills.pop(normalized, None)
        self._pending_reconciles.pop(normalized, None)
        self._liquidation_locks.pop(normalized, None)
        self._fill_timeout_warnings.pop(normalized, None)
        for mapping in (self._recent_fills, self._account_lookup_warnings):
            for key in [key for key in mapping if key[0] == normalized]:
                mapping.pop(key, None)

    def wallet_cache_addresses(self) -> set[str]:
        """返回所有钱包级进程内状态的地址快照。"""
        addresses = {
            *self._account_metadata,
            *self._deferred_fills,
            *self._pending_reconciles,
            *self._liquidation_locks,
            *self._fill_timeout_warnings,
        }
        addresses.update(address for address, _coin in self._recent_fills)
        addresses.update(address for address, _kind in self._account_lookup_warnings)
        return addresses

    async def _process_states(
        self,
        address: str,
        states: list[tuple[str, Any]],
        *,
        account_value: Decimal | None,
        allow_fill_lookup: bool,
        observed_at: datetime | None = None,
    ) -> None:
        now = observed_at or self.clock()
        async with self.engine.connect() as conn:
            baseline_at = await load_monitor_baseline(conn, address)
            last_observed_at = await load_last_observed_at(conn, address)
        establishing_baseline = baseline_at is None
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
                old = previous.get(coin) or _zero_snapshot(
                    address,
                    dex,
                    coin,
                    last_observed_at or now,
                )
                new = current.get(coin) or _zero_snapshot(address, dex, coin, now)
                old = _with_account(old, account_value, total_notional)
                new = _with_account(new, account_value, total_notional)
                transitions.append((old, new))
            responses.append((dex, state))

        changed_coins = {new.coin for old, new in transitions if old.szi != new.szi}
        fill = None
        if changed_coins and allow_fill_lookup and not establishing_baseline:
            result = await self._lookup_fill_evidence(
                address,
                changed_coins,
                recover_persisted_on_throttle=True,
            )
            fill = result.evidence
            if result.status is _FillLookupStatus.TIMEOUT:
                self._log_fill_timeout(address)

        async with self.engine.connect() as conn:
            wallet_row = (
                await conn.execute(select(hl_wallets).where(hl_wallets.c.address == address))
            ).mappings().first()
            risk_states = {
                (new.dex, new.coin): (
                    None
                    if establishing_baseline
                    else await load_liquidation_risk_state(conn, address, new.dex, new.coin)
                )
                for _, new in transitions
            }
        wallet = dict(wallet_row or {})
        events = []
        next_risks: dict[tuple[str, str], str | None] = {}
        current_snapshots: dict[tuple[str, str], PositionSnapshot] = {}
        used_fill_keys: list[tuple[str, str]] = []
        for old, new in transitions:
            current_snapshots[(new.dex, new.coin)] = new
            if fill is not None and fill.get("coin") == new.coin:
                evidence = fill
            else:
                evidence = self._recent_fills.get((address, new.coin))
            if evidence is not None:
                used_fill_keys.append((address, new.coin))
            mark_px = self.mids.get(new.coin) or _snapshot_mark_px(new) or _snapshot_mark_px(old)
            result = detect_events_with_state(
                old,
                new,
                cfg=self.cfg,
                fill=evidence,
                mark_px=mark_px,
                liquidation_risk_state=risk_states[(new.dex, new.coin)],
                wallet=wallet,
            )
            if not establishing_baseline:
                if evidence is None:
                    suffix = str(int(old.updated_at.timestamp() * 1000))
                    for event in result.events:
                        stable_suffix = (
                            f"{suffix}:{event.raw.get('liquidation_risk_level')}"
                            if event.event_type == "hl_liq_risk"
                            else suffix
                        )
                        event.dedupe_key = (
                            f"{event.dedupe_key.rsplit(':', 1)[0]}:{stable_suffix}"
                        )
                if (
                    evidence is None
                    and now - old.updated_at
                    > timedelta(seconds=self.cfg.events.max_push_age_s)
                ):
                    for event in result.events:
                        event.occurred_at = old.updated_at
                        event.raw["backfill"] = True
                events.extend(result.events)
            next_risks[(new.dex, new.coin)] = result.liquidation_risk_state

        if events and self.sink is not None:
            await self.sink.ingest(
                FetchResult(events, None),
                collector="hl_monitor",
                track_collector_state=False,
            )
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
                current = current_snapshots[(dex, coin)]
                if current.szi == 0 or current.notional_usd <= 0:
                    await delete_liquidation_risk_state(conn, address, dex, coin)
                else:
                    await save_liquidation_risk_state(
                        conn,
                        address,
                        dex,
                        coin,
                        level,
                        now,
                    )
            active_dexes = sorted(
                {
                    dex
                    for dex, state in responses
                    if dex
                    and any(
                        snapshot.szi != 0
                        for snapshot in _snapshots(state, address, dex, now)
                    )
                }
            )
            await _save_known_dexes(conn, address, active_dexes, now)
            if establishing_baseline:
                await save_monitor_baseline(conn, address, now)
                await _initialize_fill_cursor(conn, address, now)
            else:
                assert baseline_at is not None
                await save_last_observed_at(conn, address, baseline_at, now)
        if self.on_position is not None:
            try:
                await self.on_position(address, dict(self.mids))
            except Exception as exc:
                logger.error(
                    "广播钱包 %s…%s 仓位更新失败：%s",
                    address[:6],
                    address[-4:],
                    exception_summary(exc),
                )
        if establishing_baseline:
            for key in [key for key in self._recent_fills if key[0] == address]:
                self._recent_fills.pop(key, None)
            self._deferred_fills.pop(address, None)
        for key in used_fill_keys:
            self._recent_fills.pop(key, None)

    async def process_liquidation_fills(self, address: str) -> None:
        """消费清算游标之后的已入库成交，事件提交后才推进游标。"""
        if self.sink is None:
            return
        normalized = normalize_address(address)
        lock = self._liquidation_locks.setdefault(normalized, asyncio.Lock())
        async with lock:
            await self._process_liquidation_fills_locked(normalized)

    async def _process_liquidation_fills_locked(self, normalized: str) -> None:
        """在钱包级单飞锁内完成清算成交到事件及游标的提交边界。"""
        state_name = f"liquidations:{normalized}"
        async with self.engine.connect() as conn:
            baseline_at = await load_monitor_baseline(conn, normalized)
            if baseline_at is None:
                return
            cursor = await conn.scalar(
                select(hl_state.c.cursor).where(hl_state.c.name == state_name)
            )
            wallet_row = (
                await conn.execute(
                    select(hl_wallets).where(hl_wallets.c.address == normalized)
                )
            ).mappings().first()
        baseline_ms = int(baseline_at.timestamp() * 1000)
        cursor = dict(cursor) if isinstance(cursor, Mapping) else {}
        last_time_ms = max(int(cursor.get("last_time") or baseline_ms), baseline_ms)
        last_tid = int(cursor.get("last_tid") or -1)
        last_time = datetime.fromtimestamp(last_time_ms / 1000, tz=timezone.utc)
        liquidation_key = (
            literal(f"hl:{normalized}:")
            + hl_fills.c.coin
            + literal(":hl_liquidated:")
            + cast(hl_fills.c.tid, String)
        )
        event_already_exists = (
            select(event_rows.c.id)
            .where(event_rows.c.dedupe_key == liquidation_key)
            .exists()
        )
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(hl_fills)
                    .where(
                        hl_fills.c.address == normalized,
                        hl_fills.c.occurred_at >= baseline_at,
                        or_(
                            hl_fills.c.occurred_at > last_time,
                            and_(
                                hl_fills.c.occurred_at == last_time,
                                hl_fills.c.tid > last_tid,
                            ),
                        ),
                    )
                    .order_by(hl_fills.c.occurred_at, hl_fills.c.tid)
                )
            ).mappings().all()
            late_liquidations = (
                await conn.execute(
                    select(hl_fills)
                    .where(
                        hl_fills.c.address == normalized,
                        hl_fills.c.occurred_at >= baseline_at,
                        or_(
                            hl_fills.c.occurred_at < last_time,
                            and_(
                                hl_fills.c.occurred_at == last_time,
                                hl_fills.c.tid <= last_tid,
                            ),
                        ),
                        func.lower(
                            hl_fills.c.liquidation["liquidatedUser"].as_string()
                        )
                        == normalized,
                        ~event_already_exists,
                    )
                    .order_by(hl_fills.c.occurred_at, hl_fills.c.tid)
                )
            ).mappings().all()
        events = []
        wallet = dict(wallet_row or {})
        candidates = {
            int(row["tid"]): row for row in [*late_liquidations, *rows]
        }
        for row in sorted(
            candidates.values(),
            key=lambda item: (item["occurred_at"], item["tid"]),
        ):
            event = detect_liquidation_event(
                normalized,
                _fill_from_row(row),
                cfg=self.cfg,
                wallet=wallet,
            )
            if event is not None:
                events.append(event)
        if events:
            await self.sink.ingest(
                FetchResult(events, None),
                collector="hl_monitor",
                track_collector_state=False,
            )
        if rows:
            final = rows[-1]
            last_time_ms = int(final["occurred_at"].timestamp() * 1000)
            last_tid = int(final["tid"])
        cursor = {"last_time": last_time_ms, "last_tid": last_tid}
        stmt = pg_insert(hl_state).values(
            name=state_name,
            cursor=cursor,
            updated_at=self.clock(),
        )
        async with self.engine.begin() as conn:
            await conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["name"],
                    set_={
                        "cursor": stmt.excluded.cursor,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
            )

    async def process_pending_liquidations(self) -> None:
        """启动时恢复所有受监控钱包尚未完成的清算转换。"""
        if self.sink is None:
            return
        async with self.engine.connect() as conn:
            addresses = (
                await conn.execute(
                    select(hl_wallets.c.address).where(
                        hl_wallets.c.tier.in_(("focus", "core")),
                        hl_wallets.c.excluded.is_(False),
                    )
                )
            ).scalars().all()
        for address in addresses:
            await self.process_liquidation_fills(address)

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
        await self.process_liquidation_fills(address)

    async def _try_info(self, request_type: str, **params: Any) -> tuple[bool, Any]:
        if self.rest is None:
            return False, None
        method = getattr(self.rest, "try_info", None)
        if method is not None:
            return await method(request_type, budget="monitor", **params)
        return True, await self.rest.info(request_type, budget="monitor", **params)

    async def _core_account_value(self, address: str, clearing: Any) -> Decimal | None:
        """读取最近一次持久化账户元数据，不在核心 WS 处理路径发起 REST 请求。"""
        normalized = normalize_address(address)
        if normalized not in self._account_metadata:
            async with self.engine.connect() as conn:
                cursor = await conn.scalar(
                    select(hl_state.c.cursor).where(
                        hl_state.c.name == f"account_metadata:{normalized}"
                    )
                )
            if isinstance(cursor, Mapping):
                self._account_metadata[normalized] = dict(cursor)
        return self._account_value_from_metadata(normalized, clearing)

    def _account_value_from_metadata(self, address: str, clearing: Any) -> Decimal | None:
        """从最近账户类型与现货快照计算权益；资料缺失时保持待更新。"""
        metadata = self._account_metadata.get(address)
        if not metadata or "account_type" not in metadata:
            return None
        account_type = metadata["account_type"]
        if _is_unified(account_type) and not isinstance(metadata.get("spot"), Mapping):
            return None
        return combined_account_value(account_type, clearing, metadata.get("spot"))

    async def refresh_account_metadata(self, address: str) -> None:
        """用批处理预算低优先级刷新账户类型及统一账户现货权益，并持久化结果。"""
        normalized = normalize_address(address)
        if self.rest is None:
            return
        account_type = await self.rest.info(
            "userAbstraction", budget="batch", user=normalized
        )
        now = self.clock()
        metadata: dict[str, Any] = {
            "account_type": account_type,
            "account_type_updated_at": now.isoformat(),
        }
        if _is_unified(account_type):
            spot = await self.rest.info(
                "spotClearinghouseState", budget="batch", user=normalized
            )
            if isinstance(spot, Mapping):
                metadata["spot"] = dict(spot)
                metadata["spot_updated_at"] = now.isoformat()
        stmt = pg_insert(hl_state).values(
            name=f"account_metadata:{normalized}", cursor=metadata, updated_at=now
        )
        async with self.engine.begin() as conn:
            wallet = (
                await conn.execute(
                    select(hl_wallets.c.tier, hl_wallets.c.excluded).where(
                        hl_wallets.c.address == normalized
                    )
                )
            ).first()
            if (
                wallet is None
                or wallet.excluded
                or wallet.tier not in {"focus", "core"}
            ):
                self._account_metadata.pop(normalized, None)
                return
            await conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["name"],
                    set_={
                        "cursor": stmt.excluded.cursor,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
            )
        self._account_metadata[normalized] = metadata

    async def account_metadata_due(self, address: str) -> bool:
        """判断账户元数据是否缺失或超过可配置 TTL。"""
        normalized = normalize_address(address)
        metadata = self._account_metadata.get(normalized)
        if metadata is None:
            async with self.engine.connect() as conn:
                cursor = await conn.scalar(
                    select(hl_state.c.cursor).where(
                        hl_state.c.name == f"account_metadata:{normalized}"
                    )
                )
            if isinstance(cursor, Mapping):
                metadata = dict(cursor)
                self._account_metadata[normalized] = metadata
        if not metadata:
            return True
        updated_at = _parse_datetime(metadata.get("account_type_updated_at"))
        return updated_at is None or (
            self.clock() - updated_at
        ).total_seconds() >= self.cfg.frequencies.account_type_cache_s

    def _log_fill_timeout(self, address: str, *, deferred: bool = False) -> None:
        """同钱包真实成交补查超时每半小时至多记录一次 WARNING。"""
        now = self.clock()
        previous = self._fill_timeout_warnings.get(address)
        warning_interval = self.cfg.frequencies.fill_timeout_warning_interval_s
        emit_warning = (
            previous is None or (now - previous).total_seconds() >= warning_interval
        )
        log = logger.warning if emit_warning else logger.debug
        if deferred:
            timeout_s = self.deferred_fill_lookup_timeout_s
            log(
                "钱包 %s…%s 延期成交补查超过 %g 秒，将继续顺延（真实网络超时）",
                address[:6],
                address[-4:],
                timeout_s,
            )
        else:
            log(
                "钱包 %s…%s 成交补查超过 %g 秒，先按快照生成事件",
                address[:6],
                address[-4:],
                self.fill_lookup_timeout_s,
            )
        if emit_warning:
            self._fill_timeout_warnings[address] = now

    def _log_account_lookup_failure(
        self,
        address: str,
        kind: str,
        message: str,
        exc: BaseException,
    ) -> None:
        """同钱包同类账户补查失败每半小时至多记录一次 WARNING。"""
        now = self.clock()
        key = (address, kind)
        previous = self._account_lookup_warnings.get(key)
        warning_interval = self.cfg.frequencies.fill_timeout_warning_interval_s
        emit_warning = (
            previous is None or (now - previous).total_seconds() >= warning_interval
        )
        log = logger.warning if emit_warning else logger.debug
        log(message, address[:6], address[-4:], exception_summary(exc))
        if emit_warning:
            self._account_lookup_warnings[key] = now


async def _initialize_fill_cursor(
    conn: AsyncConnection,
    address: str,
    baseline_at: datetime,
) -> None:
    """把首次成交补查起点固定为监控基线，避免回扫全量历史。"""
    stmt = pg_insert(hl_state).values(
        name=f"fills:{address}",
        cursor={"last_time": int(baseline_at.timestamp() * 1000)},
        updated_at=baseline_at,
    )
    await conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
        )
    )
    liquidation_stmt = pg_insert(hl_state).values(
        name=f"liquidations:{address}",
        cursor={
            "last_time": int(baseline_at.timestamp() * 1000),
            "last_tid": -1,
        },
        updated_at=baseline_at,
    )
    await conn.execute(
        liquidation_stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={
                "cursor": liquidation_stmt.excluded.cursor,
                "updated_at": liquidation_stmt.excluded.updated_at,
            },
        )
    )


async def _save_known_dexes(
    conn: AsyncConnection,
    address: str,
    dexes: Iterable[str],
    updated_at: datetime,
) -> None:
    """持久化钱包当前仍有仓位的 HIP-3 dex。"""
    name = f"dexes:{normalize_address(address)}"
    cursor = {"dexes": sorted({str(dex) for dex in dexes if dex})}
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


def _snapshot_mark_px(snapshot: PositionSnapshot) -> Decimal | None:
    """从仓位名义与数量还原状态消息隐含的当前标记价。"""
    if snapshot.szi == 0 or snapshot.notional_usd <= 0:
        return None
    return snapshot.notional_usd / abs(snapshot.szi)


def _with_account(
    snapshot: PositionSnapshot,
    value: Decimal | None,
    notional: Decimal,
) -> PositionSnapshot:
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
        "side": str(fill.get("side") or "") or None,
        "start_position": _optional_decimal(fill.get("startPosition")),
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


def _fill_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """把持久化成交还原为事件检测使用的上游字段形状。"""
    return {
        "coin": row["coin"],
        "dir": row["dir"],
        "side": row.get("side"),
        "startPosition": row.get("start_position"),
        "px": row["px"],
        "sz": row["sz"],
        "closedPnl": row["closed_pnl"],
        "fee": row["fee"],
        "crossed": row["crossed"],
        "time": int(row["occurred_at"].timestamp() * 1000),
        "hash": row["hash"],
        "tid": row["tid"],
        "liquidation": row.get("liquidation"),
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
