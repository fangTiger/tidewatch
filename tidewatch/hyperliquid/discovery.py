"""Hyperliquid 全市场成交流解析、聚合与新巨鲸发现。"""

from __future__ import annotations

import logging
import inspect
from collections import defaultdict, deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import and_, delete, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.hyperliquid.baseline import save_monitor_baseline
from tidewatch.hyperliquid.logging import exception_summary, redact_text
from tidewatch.hyperliquid.positions import upsert_position_snapshot
from tidewatch.hyperliquid.registry import WalletRegistry, normalize_address
from tidewatch.hyperliquid.tiers import NON_PROMOTABLE_STYLES
from tidewatch.models import Event, FetchResult
from tidewatch.store.tables import hl_state, hl_wallets


logger = logging.getLogger(__name__)
ZERO_HASH = "0x" + "0" * 64
MAX_TAKER_TIMESTAMPS = 301


@dataclass(frozen=True)
class Trade:
    """一笔已确定吃单方和名义价值的公开成交。"""

    taker: str
    coin: str
    side: str
    notional_usd: Decimal
    time_ms: int
    tid: int
    trade_hash: str
    users: tuple[str, str]

    @property
    def identity(self) -> tuple[Any, ...]:
        """普通成交按链上标识去重，TWAP 切片保留各自成交编号。"""
        if self.trade_hash == ZERO_HASH:
            return ("twap", self.time_ms, self.tid, *self.users)
        return ("trade", self.trade_hash, self.tid)


@dataclass(frozen=True)
class TradeWindow:
    """同一吃单方、币种和固定三秒区间的聚合结果。"""

    address: str
    coin: str
    start_ms: int
    end_ms: int
    notional_usd: Decimal
    trade_count: int


@dataclass(frozen=True)
class QualificationRequest:
    """等待共享监控预算执行的新巨鲸资格查询。"""

    address: str
    coin: str
    start_ms: int
    end_ms: int
    notional_usd: Decimal
    trade_count: int


@dataclass
class _OpenWindow:
    notional_usd: Decimal = Decimal(0)
    trade_count: int = 0
    identities: set[tuple[Any, ...]] = field(default_factory=set)


@dataclass
class _DiscoverySummary:
    """一个可观测性输出周期内的发现流水统计。"""

    processed_trades: int = 0
    closed_windows: int = 0
    max_window: TradeWindow | None = None
    threshold_windows: int = 0
    qualification_queries: int = 0
    promotions: int = 0
    not_qualified_caches: int = 0
    high_frequency_skips: int = 0
    query_limit_deferrals: int = 0


class TradeAggregator:
    """按固定时间窗累计成交，并对重复 WS 投递保持幂等。"""

    def __init__(self, *, window_seconds: float = 3) -> None:
        if window_seconds <= 0:
            raise ValueError("成交聚合窗口必须大于 0")
        self.window_ms = int(window_seconds * 1000)
        if self.window_ms <= 0:
            raise ValueError("成交聚合窗口至少为 1 毫秒")
        self._windows: dict[tuple[str, str, int], _OpenWindow] = {}

    def add(self, trade: Trade) -> bool:
        """加入一笔成交；重复记录返回 False。"""
        start_ms = trade.time_ms // self.window_ms * self.window_ms
        key = (trade.taker, trade.coin, start_ms)
        window = self._windows.setdefault(key, _OpenWindow())
        if trade.identity in window.identities:
            return False
        window.identities.add(trade.identity)
        window.notional_usd += trade.notional_usd
        window.trade_count += 1
        return True

    def pop_due(self, through_ms: int) -> list[TradeWindow]:
        """取出结束时间不晚于给定时刻的窗口。"""
        due_keys = [
            key
            for key in self._windows
            if key[2] + self.window_ms <= through_ms
        ]
        result: list[TradeWindow] = []
        for address, coin, start_ms in sorted(due_keys, key=lambda item: (item[2], item[0], item[1])):
            window = self._windows.pop((address, coin, start_ms))
            result.append(
                TradeWindow(
                    address=address,
                    coin=coin,
                    start_ms=start_ms,
                    end_ms=start_ms + self.window_ms,
                    notional_usd=window.notional_usd,
                    trade_count=window.trade_count,
                )
            )
        return result


class TradeDiscovery:
    """消费成交窗口、过滤陌生地址并持久化资格查询结果。"""

    def __init__(
        self,
        cfg: Any,
        *,
        engine: AsyncEngine,
        registry: WalletRegistry,
        monitor: Any,
        rest: Any,
        sink: Any,
        clock: Clock = utcnow,
        schedule_query: Callable[[QualificationRequest], Any] | None = None,
        trigger_reconcile: Callable[[str], Any] | None = None,
    ) -> None:
        self.cfg = cfg
        self.engine = engine
        self.registry = registry
        self.monitor = monitor
        self.rest = rest
        self.sink = sink
        self.clock = clock
        self.schedule_query = schedule_query
        self.trigger_reconcile = trigger_reconcile
        self.aggregator = TradeAggregator(
            window_seconds=cfg.frequencies.discovery_window_s
        )
        self._taker_times: dict[str, deque[int]] = defaultdict(deque)
        self._processed: dict[str, datetime] = {}
        self._queued: dict[str, datetime] = {}
        self._summary = _DiscoverySummary()
        self._summary_started_at = self.clock()

    async def handle_message(self, message: Any) -> None:
        """解析一批成交流消息并更新窗口与一小时吃单计数。"""
        trades = parse_trade_message(message)
        self._summary.processed_trades += len(trades)
        for trade in trades:
            cutoff = trade.time_ms - 3_600_000
            times = self._taker_times[trade.taker]
            while times and times[0] < cutoff:
                times.popleft()
            times.append(trade.time_ms)
            while len(times) > MAX_TAKER_TIMESTAMPS:
                times.popleft()
            self.aggregator.add(trade)

    async def flush_due(self, through_ms: int) -> None:
        """关闭到期窗口，并按名单、风格、频率与缓存决定后续动作。"""
        self._evict_taker_times(through_ms)
        threshold = Decimal(str(self.cfg.events.discovery_trade_notional_usd))
        windows = self.aggregator.pop_due(through_ms)
        self._summary.closed_windows += len(windows)
        for window in windows:
            if (
                self._summary.max_window is None
                or window.notional_usd > self._summary.max_window.notional_usd
            ):
                self._summary.max_window = window
            if window.notional_usd < threshold:
                continue
            self._summary.threshold_windows += 1
            request = QualificationRequest(**window.__dict__)
            name = self._window_state_name(request)
            if (
                name in self._processed
                or name in self._queued
                or await self._window_already_handled(name)
            ):
                self._processed[name] = self.clock()
                continue
            # 达标窗口的首个落盘状态就是 queued；后续任何读库、过滤或调度失败，
            # 重启都能从这一状态恢复资格查询。
            await self._write_window(request, status="queued", processed=False)
            wallet = await self._wallet(window.address)
            if wallet is not None and wallet["tier"] in {"focus", "core"}:
                if self.trigger_reconcile is not None:
                    await _invoke(self.trigger_reconcile, window.address)
                await self.mark_processed(request, outcome="monitored")
                continue
            if wallet is not None and wallet["style"] in NON_PROMOTABLE_STYLES:
                self._summary.high_frequency_skips += 1
                logger.warning(
                    "跳过已标记做市/高频的钱包 %s…%s 新巨鲸资格查询",
                    window.address[:6],
                    window.address[-4:],
                )
                await self.mark_processed(request, outcome="known_high_frequency")
                continue
            if len(self._taker_times.get(window.address, ())) > 300:
                self._summary.high_frequency_skips += 1
                logger.warning(
                    "跳过一小时吃单超过 300 笔的高频陌生钱包 %s…%s",
                    window.address[:6],
                    window.address[-4:],
                )
                await self.mark_processed(request, outcome="stream_high_frequency")
                continue
            if not await self.may_schedule(window.address):
                await self.mark_processed(request, outcome="cached_not_qualified")
                continue
            if self.schedule_query is None:
                await self.process_qualification(request)
            else:
                await _invoke(self.schedule_query, request)
                self._queued[name] = self.clock()
        self._maybe_log_summary()

    def record_query_limit_deferral(self) -> None:
        """记录一次发现资格查询因每分钟上限而顺延。"""
        self._summary.query_limit_deferrals += 1

    async def cleanup_expired_state(self) -> int:
        """删除过期发现状态，并同步淘汰进程内幂等索引。"""
        now = self.clock()
        window_cutoff = now - timedelta(
            seconds=self.cfg.frequencies.discovery_state_retention_s
        )
        cache_cutoff = now - timedelta(
            seconds=self.cfg.frequencies.discovery_cache_s
        )
        stmt = delete(hl_state).where(
            or_(
                and_(
                    hl_state.c.name.like("discovery_window:%"),
                    hl_state.c.updated_at <= window_cutoff,
                ),
                and_(
                    hl_state.c.name.like("discovery_cache:%"),
                    hl_state.c.updated_at <= cache_cutoff,
                ),
            )
        )
        async with self.engine.begin() as conn:
            result = await conn.execute(stmt)
        for memory in (self._processed, self._queued):
            expired = [
                name
                for name, updated_at in memory.items()
                if updated_at <= window_cutoff
            ]
            for name in expired:
                memory.pop(name, None)
        return result.rowcount or 0

    async def restore_pending(self) -> None:
        """重启时恢复已持久化但尚未完成的资格查询。"""
        if self.schedule_query is None:
            return
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(hl_state.c.name, hl_state.c.cursor).where(
                        hl_state.c.name.like("discovery_window:%")
                    )
                )
            ).all()
        for name, cursor in rows:
            if (
                not isinstance(cursor, Mapping)
                or cursor.get("processed")
                or cursor.get("status") != "queued"
                or name in self._queued
            ):
                continue
            try:
                request = QualificationRequest(
                    address=normalize_address(cursor["address"]),
                    coin=str(cursor["coin"]),
                    start_ms=int(cursor["start_ms"]),
                    end_ms=int(cursor["end_ms"]),
                    notional_usd=_decimal(cursor["notional_usd"]),
                    trade_count=int(cursor["trade_count"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning(
                    "忽略损坏的发现查询游标 %s：%s",
                    redact_text(name),
                    exception_summary(exc),
                )
                continue
            await _invoke(self.schedule_query, request)
            self._queued[name] = self.clock()

    async def may_schedule(self, address: str) -> bool:
        """未达标缓存过期后才允许重新安排资格查询。"""
        normalized = normalize_address(address)
        async with self.engine.connect() as conn:
            cursor = await conn.scalar(
                select(hl_state.c.cursor).where(
                    hl_state.c.name == f"discovery_cache:{normalized}"
                )
            )
        if not isinstance(cursor, Mapping):
            return True
        expires_at = _parse_datetime(cursor.get("expires_at"))
        return expires_at is None or self.clock() >= expires_at

    async def process_qualification(self, request: QualificationRequest) -> bool:
        """用共享 REST 客户端完成资格查询；预算不足时返回 False 供队列顺延。"""
        if self.rest is None:
            return False
        self._summary.qualification_queries += 1
        params: dict[str, Any] = {"user": request.address}
        if ":" in request.coin:
            params["dex"] = request.coin.split(":", 1)[0]
        method = getattr(self.rest, "try_info", None)
        if method is not None:
            available, state = await method(
                "clearinghouseState", budget="monitor", **params
            )
            if not available:
                return False
        else:
            state = await self.rest.info(
                "clearinghouseState", budget="monitor", **params
            )
        position_notional, total_notional, account_value = _qualification_values(
            state, request.coin
        )
        position_threshold = Decimal(str(self.cfg.events.discovery_trade_notional_usd))
        account_threshold = Decimal(str(self.cfg.events.discovery_account_value_usd))
        if position_notional < position_threshold and account_value < account_threshold:
            await self._cache_not_qualified(request.address)
            self._summary.not_qualified_caches += 1
            await self.mark_processed(request, outcome="not_qualified")
            return True

        occurred_at = datetime.fromtimestamp(request.end_ms / 1000, tz=timezone.utc)
        promotion = await self.registry.promote_discovered(
            request.address,
            total_notional=total_notional,
            last_trade_at=occurred_at,
        )
        if promotion.promoted:
            self._summary.promotions += 1
            dex = request.coin.split(":", 1)[0] if ":" in request.coin else ""
            async with self.engine.begin() as conn:
                await upsert_position_snapshot(
                    conn,
                    address=request.address,
                    dex=dex,
                    response=state,
                    updated_at=occurred_at,
                )
                await save_monitor_baseline(conn, request.address, occurred_at)
            if self.sink is not None:
                event = Event(
                    source="hl_discovery",
                    category="hyperliquid",
                    event_type="hl_new_whale",
                    occurred_at=occurred_at,
                    title=f"Hyperliquid 发现新巨鲸 {request.coin}",
                    dedupe_key=(
                        f"hl:{request.address}:{request.coin}:hl_new_whale:{request.end_ms}"
                    ),
                    assets=[request.coin],
                    amount_usd=request.notional_usd,
                    raw={
                        "address": request.address,
                        "coin": request.coin,
                        "tier": "focus",
                        "style": None,
                        "notional_usd": str(request.notional_usd),
                        "position_notional_usd": str(position_notional),
                        "account_value_usd": str(account_value),
                        "trade_count": request.trade_count,
                        "replaced_address": promotion.replaced,
                    },
                )
                await self.sink.ingest(
                    FetchResult([event], None),
                    collector="hl_discovery",
                    track_collector_state=False,
                )
            outcome = "qualified"
        else:
            outcome = promotion.reason or "not_promoted"
            logger.warning(
                "新巨鲸钱包 %s…%s 达标但未进入重点层：%s",
                request.address[:6],
                request.address[-4:],
                outcome,
            )
        await self.mark_processed(request, outcome=outcome)
        return True

    async def mark_processed(
        self,
        request: QualificationRequest,
        *,
        outcome: str,
    ) -> None:
        """持久化窗口终态，保证重启回放不会重复处理。"""
        name = self._window_state_name(request)
        self._processed[name] = self.clock()
        self._queued.pop(name, None)
        await self._write_window(
            request,
            status=outcome,
            processed=True,
        )

    async def _cache_not_qualified(self, address: str) -> None:
        now = self.clock()
        expires_at = now + timedelta(seconds=self.cfg.frequencies.discovery_cache_s)
        name = f"discovery_cache:{normalize_address(address)}"
        stmt = pg_insert(hl_state).values(
            name=name,
            cursor={"expires_at": expires_at.isoformat()},
            updated_at=now,
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

    async def _wallet(self, address: str) -> Mapping[str, Any] | None:
        async with self.engine.connect() as conn:
            return (
                await conn.execute(
                    select(hl_wallets).where(hl_wallets.c.address == address)
                )
            ).mappings().first()

    async def _window_already_handled(self, name: str) -> bool:
        async with self.engine.connect() as conn:
            cursor = await conn.scalar(
                select(hl_state.c.cursor).where(hl_state.c.name == name)
            )
        return isinstance(cursor, Mapping) and bool(cursor.get("processed"))

    def _evict_taker_times(self, through_ms: int) -> None:
        """淘汰一小时窗口外的吃单计数及不再活跃的地址键。"""
        cutoff = through_ms - 3_600_000
        inactive: list[str] = []
        for address, times in self._taker_times.items():
            while times and times[0] < cutoff:
                times.popleft()
            if not times:
                inactive.append(address)
        for address in inactive:
            self._taker_times.pop(address, None)

    def _maybe_log_summary(self) -> None:
        """到达配置周期时输出发现摘要，并从零开始下一周期。"""
        now = self.clock()
        interval = self.cfg.frequencies.discovery_summary_interval_s
        if (now - self._summary_started_at).total_seconds() < interval:
            return
        maximum = self._summary.max_window
        if maximum is None:
            maximum_text = "无"
        else:
            maximum_text = (
                f"{maximum.notional_usd} 美元（{maximum.coin}，"
                f"{maximum.address[:6]}…{maximum.address[-4:]}）"
            )
        logger.info(
            "成交流发现摘要：处理成交 %d 笔，关闭窗口 %d 个，最大窗口名义 %s，"
            "≥门槛窗口 %d 个，资格查询 %d 次，达标晋升 %d 个，不达标缓存 %d 个，"
            "高频跳过 %d 个，因查询限额顺延 %d 次",
            self._summary.processed_trades,
            self._summary.closed_windows,
            maximum_text,
            self._summary.threshold_windows,
            self._summary.qualification_queries,
            self._summary.promotions,
            self._summary.not_qualified_caches,
            self._summary.high_frequency_skips,
            self._summary.query_limit_deferrals,
        )
        self._summary = _DiscoverySummary()
        self._summary_started_at = now

    async def _write_window(
        self,
        request: QualificationRequest,
        *,
        status: str,
        processed: bool,
    ) -> None:
        now = self.clock()
        name = self._window_state_name(request)
        cursor = {
            "address": request.address,
            "coin": request.coin,
            "start_ms": request.start_ms,
            "end_ms": request.end_ms,
            "notional_usd": str(request.notional_usd),
            "trade_count": request.trade_count,
            "status": status,
            "processed": processed,
        }
        stmt = pg_insert(hl_state).values(name=name, cursor=cursor, updated_at=now)
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

    @staticmethod
    def _window_state_name(request: QualificationRequest) -> str:
        return (
            f"discovery_window:{request.end_ms}:{request.address}:{request.coin}"
        )


def parse_trade_message(message: Any) -> list[Trade]:
    """解析官方 ``trades`` WS 消息并确定每笔成交的吃单方。"""
    if not isinstance(message, Mapping) or message.get("channel") != "trades":
        raise ValueError("成交流消息 channel 必须是 trades")
    raw_trades = message.get("data")
    if not isinstance(raw_trades, list):
        raise ValueError("成交流消息 data 必须是数组")
    trades: list[Trade] = []
    for raw in raw_trades:
        if not isinstance(raw, Mapping):
            raise ValueError("成交记录必须是对象")
        users = raw.get("users")
        if not isinstance(users, list) or len(users) != 2:
            raise ValueError("成交记录 users 必须包含买卖双方")
        side = str(raw.get("side") or "")
        if side not in {"A", "B"}:
            raise ValueError("成交方向必须是 A 或 B")
        try:
            px = Decimal(str(raw["px"]))
            size = Decimal(str(raw["sz"]))
            time_ms = int(raw["time"])
            tid = int(raw["tid"])
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("成交记录缺少有效 px、sz、time 或 tid") from exc
        if not px.is_finite() or not size.is_finite() or px <= 0 or size <= 0:
            raise ValueError("成交价格和数量必须是有限正数")
        normalized_users = (normalize_address(users[0]), normalize_address(users[1]))
        taker = normalized_users[0] if side == "B" else normalized_users[1]
        coin = str(raw.get("coin") or "")
        if not coin:
            raise ValueError("成交币种不能为空")
        trades.append(
            Trade(
                taker=taker,
                coin=coin,
                side=side,
                notional_usd=px * size,
                time_ms=time_ms,
                tid=tid,
                trade_hash=str(raw.get("hash") or ""),
                users=normalized_users,
            )
        )
    return trades


async def load_trade_coins(rest: Any, hip3_allowlist: Iterable[str]) -> tuple[str, ...]:
    """用官方 ``perpDexs`` 校验白名单，再从各 dex ``meta`` 取活跃币种。"""
    raw_dexes = await rest.info("perpDexs", budget="monitor")
    if not isinstance(raw_dexes, list):
        raise ValueError("perpDexs 响应必须是数组")
    official = {
        str(item["name"])
        for item in raw_dexes
        if isinstance(item, Mapping) and isinstance(item.get("name"), str)
    }
    configured = tuple(dict.fromkeys(str(dex) for dex in hip3_allowlist))
    unknown = sorted(set(configured) - official)
    if unknown:
        logger.warning("忽略官方 perpDexs 中不存在的 HIP-3 dex 白名单项：%s", ", ".join(unknown))

    coins: set[str] = set()
    main = await rest.info("meta", budget="monitor")
    coins.update(_active_meta_coins(main))
    for dex in configured:
        if dex not in official:
            continue
        meta = await rest.info("meta", budget="monitor", dex=dex)
        coins.update(_active_meta_coins(meta))
    return tuple(sorted(coins))


def _active_meta_coins(meta: Any) -> set[str]:
    if not isinstance(meta, Mapping) or not isinstance(meta.get("universe"), list):
        raise ValueError("meta.universe 必须是数组")
    return {
        str(item["name"])
        for item in meta["universe"]
        if isinstance(item, Mapping)
        and isinstance(item.get("name"), str)
        and item["name"]
        and not item.get("isDelisted", False)
    }


def _qualification_values(state: Any, coin: str) -> tuple[Decimal, Decimal, Decimal]:
    """返回触发币仓位名义、总仓位名义与合约账户价值。"""
    if not isinstance(state, Mapping):
        raise ValueError("clearinghouseState 响应必须是对象")
    summary = state.get("marginSummary")
    if not isinstance(summary, Mapping):
        raise ValueError("clearinghouseState.marginSummary 必须是对象")
    account_value = _decimal(summary.get("accountValue") or 0)
    positions = state.get("assetPositions")
    if not isinstance(positions, list):
        raise ValueError("clearinghouseState.assetPositions 必须是数组")
    position_notional = Decimal(0)
    calculated_total = Decimal(0)
    for item in positions:
        position = item.get("position") if isinstance(item, Mapping) else None
        if not isinstance(position, Mapping):
            raise ValueError("clearinghouseState.position 必须是对象")
        value = abs(_decimal(position.get("positionValue") or 0))
        calculated_total += value
        if position.get("coin") == coin:
            position_notional += value
    total_raw = summary.get("totalNtlPos")
    total_notional = (
        abs(_decimal(total_raw)) if total_raw is not None else calculated_total
    )
    return position_notional, total_notional, account_value


def _decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"数值无效：{value}") from exc
    if not parsed.is_finite():
        raise ValueError(f"数值无效：{value}")
    return parsed


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


async def _invoke(callback: Callable[..., Any], *args: Any) -> Any:
    result = callback(*args)
    if inspect.isawaitable(result):
        return await result
    return result
