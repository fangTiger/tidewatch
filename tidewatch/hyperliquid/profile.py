"""Hyperliquid 钱包画像指标、成交增量与每日批处理。"""

from __future__ import annotations

import asyncio
import logging
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.hyperliquid.monitor import combined_account_value
from tidewatch.hyperliquid.registry import normalize_address
from tidewatch.store.tables import hl_fills, hl_state, hl_wallets


logger = logging.getLogger(__name__)

SHANGHAI = ZoneInfo("Asia/Shanghai")
PROFILE_BATCH_STATE = "profile_batch"
PROFILE_FILL_PREFIX = "profile_fills:"
PROFILE_WINDOW = timedelta(days=30)
DEFAULT_FILL_PAGE_SIZE = 2_000
DEFAULT_FILL_MAX_PAGES = 5
MIN_STYLE_FILLS = 20
MIN_STYLE_ROUNDS = 2


@dataclass(frozen=True)
class FillCoverage:
    """画像成交样本的真实覆盖范围。"""

    start_at: datetime
    end_at: datetime
    complete: bool
    sample_size: int


@dataclass(frozen=True)
class ProfileComputation:
    """一次纯指标计算结果。"""

    profile: dict[str, Any]
    style: str


@dataclass(frozen=True)
class BatchEstimate:
    """按基础权重估算的一次批次规模。"""

    wallet_count: int
    weight: int
    minutes: float


@dataclass(frozen=True)
class ProfileBatchResult:
    """一次画像批次结果。"""

    processed: int
    succeeded: int
    failed: tuple[str, ...]


def seconds_until_shanghai_midnight(now: datetime) -> float:
    """返回从当前时刻到下一个上海时间 00:00 的秒数。"""
    local = now.astimezone(SHANGHAI)
    target = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if target < local:
        target += timedelta(days=1)
    return (target - local).total_seconds()


def estimate_batch_duration(
    *,
    focus_count: int,
    candidate_count: int,
    candidate_shards: int,
    weight_per_wallet: int,
    budget_per_minute: float,
) -> BatchEstimate:
    """估算重点全量加候选分片所需的最短时间。"""
    if candidate_shards <= 0 or weight_per_wallet <= 0 or budget_per_minute <= 0:
        raise ValueError("画像批次估算参数必须为正数")
    wallet_count = focus_count + math.ceil(candidate_count / candidate_shards)
    weight = wallet_count * weight_per_wallet
    return BatchEstimate(wallet_count, weight, weight / budget_per_minute)


def classify_style(
    passive_ratio: float | None,
    daily_fill_count: float | None,
    median_holding_s: float | None,
    fill_sample_size: int,
    round_sample_size: int,
) -> str:
    """按严格大于边界和最小样本量判定钱包风格。"""
    if (
        fill_sample_size >= MIN_STYLE_FILLS
        and passive_ratio is not None
        and daily_fill_count is not None
        and passive_ratio > 0.8
        and daily_fill_count > 1_000
    ):
        return "high_frequency"
    if round_sample_size < MIN_STYLE_ROUNDS or median_holding_s is None:
        return "unknown"
    if median_holding_s > 12 * 3600:
        return "swing"
    return "directional"


def rebuild_holding_durations(fills: Sequence[Mapping[str, Any]]) -> tuple[list[float], int]:
    """按币种用 ``startPosition`` 重建完整 0→持仓→0 轮次。"""
    durations, open_rounds, _ = _rebuild_holding_rounds(fills)
    return durations, open_rounds


def _rebuild_holding_rounds(
    fills: Sequence[Mapping[str, Any]],
) -> tuple[list[float], int, int]:
    """重建持仓轮次，并返回因旧字段缺失而无法重建的成交数。"""
    ordered = sorted(
        fills,
        key=lambda fill: (int(fill.get("time") or _timestamp_ms(fill)), int(fill.get("tid") or 0)),
    )
    opened_at: dict[str, int] = {}
    durations: list[float] = []
    incomplete = 0
    for fill in ordered:
        coin = str(fill.get("coin") or "")
        start = _optional_decimal(fill.get("startPosition", fill.get("start_position")))
        size = _optional_decimal(fill.get("sz"))
        side = str(fill.get("side") or "")
        time_ms = int(fill.get("time") or _timestamp_ms(fill))
        if not coin or start is None or size is None or side not in {"A", "B"} or time_ms <= 0:
            incomplete += 1
            continue
        end = start + (size if side == "B" else -size)
        if start == 0 and end != 0:
            opened_at[coin] = time_ms
            continue
        if start != 0 and end == 0:
            began = opened_at.pop(coin, None)
            if began is not None and time_ms >= began:
                durations.append((time_ms - began) / 1000)
            continue
        if start * end < 0:
            began = opened_at.get(coin)
            if began is not None and time_ms >= began:
                durations.append((time_ms - began) / 1000)
            opened_at[coin] = time_ms
    return durations, len(opened_at), incomplete


def calculate_wallet_profile(
    *,
    portfolio: Any,
    fills: Sequence[Mapping[str, Any]],
    clearing_state: Any,
    abstraction: Any,
    spot_state: Any,
    coverage: FillCoverage,
    now: datetime,
    dex_clearing_states: Mapping[str, Any] | None = None,
) -> ProfileComputation:
    """以可序列化值计算完整画像，并为每项指标附带样本披露。"""
    if now.tzinfo is None:
        raise ValueError("画像计算时间必须带时区")
    windows = _portfolio_windows(portfolio)
    all_time = windows.get("allTime")
    month = windows.get("month")
    dex_states = dict(dex_clearing_states or {})
    account_value = combined_account_value(abstraction, clearing_state, spot_state) + sum(
        (_summary_decimal(state, "accountValue") or Decimal(0) for state in dex_states.values()),
        Decimal(0),
    )
    total_notional = sum(
        (
            _summary_decimal(state, "totalNtlPos") or Decimal(0)
            for state in (clearing_state, *dex_states.values())
        ),
        Decimal(0),
    )
    total_pnl = _history_delta(all_time, "pnlHistory")
    pnl_30d = _history_delta(month, "pnlHistory")
    volume_total = _window_decimal(all_time, "vlm")
    volume_30d = _window_decimal(month, "vlm")
    drawdown, drawdown_ratio, drawdown_samples, drawdown_ratio_reason = _max_drawdown(
        all_time
    )

    close_pnls = [
        _decimal(fill.get("closedPnl", fill.get("closed_pnl", 0)))
        for fill in fills
        if _is_closing_fill(fill)
    ]
    wins = [value for value in close_pnls if value > 0]
    losses = [value for value in close_pnls if value < 0]
    win_rate = len(wins) / len(close_pnls) if close_pnls else None
    profit_loss_ratio = None
    if wins and losses:
        average_win = sum(wins, Decimal(0)) / len(wins)
        average_loss = abs(sum(losses, Decimal(0)) / len(losses))
        if average_loss:
            profit_loss_ratio = average_win / average_loss

    durations, open_rounds, incomplete_holding_fills = _rebuild_holding_rounds(fills)
    holding_complete = coverage.complete and incomplete_holding_fills == 0
    average_duration = (
        sum(durations) / len(durations)
        if durations and incomplete_holding_fills == 0
        else None
    )
    median_duration = (
        float(median(durations))
        if durations and incomplete_holding_fills == 0
        else None
    )

    notionals: dict[str, Decimal] = defaultdict(Decimal)
    passive = 0
    for fill in fills:
        coin = str(fill.get("coin") or "")
        if coin:
            notionals[coin] += abs(_decimal(fill.get("px") or 0) * _decimal(fill.get("sz") or 0))
        if not bool(fill.get("crossed")):
            passive += 1
    preferred = [
        {"coin": coin, "notional_usd": _decimal_text(value)}
        for coin, value in sorted(notionals.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]
    passive_ratio = passive / len(fills) if fills else None
    coverage_seconds = max(0.0, (coverage.end_at - coverage.start_at).total_seconds())
    daily_fill_count = (
        len(fills) / (coverage_seconds / 86400)
        if fills and coverage_seconds > 0
        else None
    )
    actual_leverage = total_notional / account_value if account_value > 0 else None
    configured_leverage = _configured_leverage(clearing_state)
    configured_leverage_notionals = _configured_leverage_notionals(clearing_state)
    for dex, state in dex_states.items():
        configured_leverage.update(_configured_leverage(state, dex=dex))
        configured_leverage_notionals.update(
            _configured_leverage_notionals(state, dex=dex)
        )
    style = classify_style(
        passive_ratio,
        daily_fill_count,
        median_duration,
        len(fills),
        len(durations),
    )

    fill_window = _coverage(
        coverage.start_at,
        coverage.end_at,
        len(fills),
        sufficient=bool(fills),
        complete=coverage.complete,
        reason=None if fills else "没有成交历史",
    )
    profile: dict[str, Any] = {
        "account_value": _decimal_text(account_value),
        "total_pnl": _decimal_text(total_pnl),
        "pnl_30d": _decimal_text(pnl_30d),
        "volume_total": _decimal_text(volume_total),
        "volume_30d": _decimal_text(volume_30d),
        "max_drawdown": _decimal_text(drawdown),
        "max_drawdown_ratio": _decimal_text(drawdown_ratio),
        "win_rate": win_rate,
        "profit_loss_ratio": _decimal_text(profit_loss_ratio),
        "holding_duration_avg_s": average_duration,
        "holding_duration_median_s": median_duration,
        "open_rounds": open_rounds,
        "preferred_coins": preferred,
        "daily_fill_count": daily_fill_count,
        "passive_fill_ratio": passive_ratio,
        "actual_leverage": _decimal_text(actual_leverage),
        "configured_leverage": configured_leverage,
        "configured_leverage_notionals": configured_leverage_notionals,
        "generated_at": now.isoformat(),
        "coverage": {
            "account_value": _coverage(
                now,
                now,
                1 + len(dex_states) + (1 if _is_unified(abstraction) else 0),
            ),
            "total_pnl": _portfolio_coverage(all_time, "pnlHistory"),
            "pnl_30d": _portfolio_coverage(month, "pnlHistory", expected_days=30),
            "volume_total": _portfolio_coverage(all_time, "pnlHistory"),
            "volume_30d": _portfolio_coverage(month, "pnlHistory", expected_days=30),
            "max_drawdown": _drawdown_coverage(
                all_time,
                sample_size=drawdown_samples,
                ratio_reason=drawdown_ratio_reason,
            ),
            "win_rate": _coverage(
                coverage.start_at,
                coverage.end_at,
                len(close_pnls),
                sufficient=bool(close_pnls),
                complete=coverage.complete,
                reason=None if close_pnls else "没有平仓类成交",
            ),
            "profit_loss_ratio": _coverage(
                coverage.start_at,
                coverage.end_at,
                len(close_pnls),
                sufficient=profit_loss_ratio is not None,
                complete=coverage.complete,
                reason=None if profit_loss_ratio is not None else "需要同时包含盈利和亏损平仓",
            ),
            "holding_duration": _coverage(
                coverage.start_at,
                coverage.end_at,
                len(durations),
                sufficient=bool(durations) and incomplete_holding_fills == 0,
                complete=holding_complete,
                reason=(
                    f"{incomplete_holding_fills} 笔成交缺少 side 或 startPosition，无法完整重建"
                    if incomplete_holding_fills
                    else None if durations else "没有完整持仓轮次"
                ),
            ),
            "preferred_coins": fill_window,
            "daily_fill_count": fill_window,
            "actual_leverage": _coverage(
                now,
                now,
                1 + len(dex_states),
                sufficient=account_value > 0,
                reason=None if account_value > 0 else "账户价值为零",
            ),
            "configured_leverage": _coverage(
                now,
                now,
                len(configured_leverage),
                sufficient=bool(configured_leverage),
                reason=None if configured_leverage else "当前没有持仓",
            ),
        },
    }
    return ProfileComputation(profile, style)


class WalletProfiler:
    """使用批处理预算抓取并保存单钱包画像。"""

    def __init__(
        self,
        engine: AsyncEngine,
        rest: Any,
        *,
        clock: Clock = utcnow,
        fill_page_size: int = DEFAULT_FILL_PAGE_SIZE,
        fill_max_pages: int = DEFAULT_FILL_MAX_PAGES,
        hip3_dexes: Sequence[str] = (),
    ) -> None:
        self.engine = engine
        self.rest = rest
        self.clock = clock
        self.fill_page_size = fill_page_size
        self.fill_max_pages = fill_max_pages
        self.hip3_dexes = tuple(hip3_dexes)

    async def sync_fills(
        self,
        address: str,
        *,
        start_at: datetime,
        end_at: datetime,
    ) -> FillCoverage:
        """分页增量成交；每页成交与游标在同一事务提交。"""
        normalized = normalize_address(address)
        if start_at.tzinfo is None or end_at.tzinfo is None:
            raise ValueError("成交回溯时间必须带时区")
        state_name = f"{PROFILE_FILL_PREFIX}{normalized}"
        async with self.engine.connect() as conn:
            stored = await conn.scalar(
                select(hl_state.c.cursor).where(hl_state.c.name == state_name)
            )
        cursor = dict(stored) if isinstance(stored, Mapping) else {}
        requested_start_ms = int(start_at.timestamp() * 1000)
        end_ms = int(end_at.timestamp() * 1000)
        current_start = max(requested_start_ms, int(cursor.get("last_time") or 0))
        observed_min = _optional_int(cursor.get("observed_min"))
        observed_max = _optional_int(cursor.get("observed_max"))
        request_complete = False
        incomplete_before = _optional_int(cursor.get("incomplete_before"))
        if incomplete_before is not None and requested_start_ms >= incomplete_before:
            incomplete_before = None
        request_observed_min: int | None = None
        seen_tids: set[int] = set()

        for _ in range(self.fill_max_pages):
            raw = await self.rest.info(
                "userFillsByTime",
                budget="batch",
                user=normalized,
                startTime=current_start,
                endTime=end_ms,
            )
            if not isinstance(raw, list):
                raise ValueError("userFillsByTime 响应必须是数组")
            page = [fill for fill in raw if isinstance(fill, Mapping)]
            if len(page) != len(raw):
                raise ValueError("userFillsByTime 成交记录必须是对象")
            times = [int(fill.get("time") or 0) for fill in page]
            if any(value <= 0 for value in times):
                raise ValueError("userFillsByTime 成交记录缺少有效 time")
            if times:
                page_min = min(times)
                page_max = max(times)
                request_observed_min = (
                    page_min
                    if request_observed_min is None
                    else min(request_observed_min, page_min)
                )
                observed_min = page_min if observed_min is None else min(observed_min, page_min)
                observed_max = page_max if observed_max is None else max(observed_max, page_max)
            last_time = max(int(cursor.get("last_time") or 0), max(times, default=0))
            request_complete = len(page) < self.fill_page_size
            cursor.update(
                {
                    "requested_start_at": start_at.isoformat(),
                    "requested_end_at": end_at.isoformat(),
                    "last_time": last_time,
                    "observed_min": observed_min,
                    "observed_max": observed_max,
                    "complete": False,
                }
            )
            tids = {int(fill["tid"]) for fill in page}
            if page and not (tids - seen_tids):
                break
            seen_tids.update(tids)
            async with self.engine.begin() as conn:
                await self._store_fill_page(conn, normalized, page)
                await _save_state(conn, state_name, cursor, self.clock())
            if request_complete:
                break
            next_start = last_time
            if next_start < current_start:
                break
            current_start = next_start

        if not request_complete and request_observed_min is not None:
            incomplete_before = (
                request_observed_min
                if incomplete_before is None
                else max(incomplete_before, request_observed_min)
            )
        complete = request_complete and incomplete_before is None
        if incomplete_before is None:
            cursor.pop("incomplete_before", None)
        else:
            cursor["incomplete_before"] = incomplete_before
        cursor["complete"] = complete
        async with self.engine.begin() as conn:
            await _save_state(conn, state_name, cursor, self.clock())

        async with self.engine.connect() as conn:
            stored_range = (
                await conn.execute(
                    select(
                        func.min(hl_fills.c.occurred_at),
                        func.max(hl_fills.c.occurred_at),
                        func.count(),
                    ).where(
                        hl_fills.c.address == normalized,
                        hl_fills.c.occurred_at >= start_at,
                        hl_fills.c.occurred_at <= end_at,
                    )
                )
            ).one()
        if complete:
            coverage_start = start_at
            coverage_end = end_at
        elif stored_range[0] is not None and stored_range[1] is not None:
            coverage_start = stored_range[0]
            coverage_end = stored_range[1]
        else:
            coverage_start = start_at
            coverage_end = end_at
        return FillCoverage(coverage_start, coverage_end, complete, int(stored_range[2] or 0))

    async def _store_fill_page(
        self,
        conn: AsyncConnection,
        address: str,
        fills: Sequence[Mapping[str, Any]],
    ) -> None:
        for fill in fills:
            row = _fill_row(address, fill)
            stmt = pg_insert(hl_fills).values(**row)
            await conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["address", "tid"],
                    set_={
                        "side": func.coalesce(hl_fills.c.side, stmt.excluded.side),
                        "start_position": func.coalesce(
                            hl_fills.c.start_position,
                            stmt.excluded.start_position,
                        ),
                    },
                )
            )

    async def _backfill_missing_fill_fields(
        self,
        address: str,
        *,
        start_at: datetime,
        end_at: datetime,
    ) -> int:
        """按缺字段旧成交的实际时间段分页回拉，并仅更新两个新增字段。"""
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(
                        hl_fills.c.tid,
                        hl_fills.c.occurred_at,
                        hl_fills.c.side,
                        hl_fills.c.start_position,
                    ).where(
                        hl_fills.c.address == address,
                        hl_fills.c.occurred_at >= start_at,
                        hl_fills.c.occurred_at <= end_at,
                        or_(
                            hl_fills.c.side.is_(None),
                            hl_fills.c.start_position.is_(None),
                        ),
                    )
                )
            ).mappings().all()
        if not rows:
            return 0

        missing = {
            int(row["tid"]): {
                "side": row["side"],
                "start_position": row["start_position"],
            }
            for row in rows
        }
        current_start = int(min(row["occurred_at"] for row in rows).timestamp() * 1000)
        end_ms = int(max(row["occurred_at"] for row in rows).timestamp() * 1000)
        seen_tids: set[int] = set()

        for _ in range(self.fill_max_pages):
            raw = await self.rest.info(
                "userFillsByTime",
                budget="batch",
                user=address,
                startTime=current_start,
                endTime=end_ms,
            )
            if not isinstance(raw, list):
                raise ValueError("userFillsByTime 响应必须是数组")
            page = [fill for fill in raw if isinstance(fill, Mapping)]
            if len(page) != len(raw):
                raise ValueError("userFillsByTime 成交记录必须是对象")
            times = [int(fill.get("time") or 0) for fill in page]
            if any(value <= 0 for value in times):
                raise ValueError("userFillsByTime 成交记录缺少有效 time")

            async with self.engine.begin() as conn:
                for fill in page:
                    try:
                        tid = int(fill["tid"])
                    except (KeyError, TypeError, ValueError) as exc:
                        raise ValueError("userFillsByTime 成交记录缺少有效 tid") from exc
                    stored = missing.get(tid)
                    if stored is None:
                        continue
                    values: dict[str, Any] = {}
                    side = str(fill.get("side") or "") or None
                    start_position = _optional_decimal(fill.get("startPosition"))
                    if stored["side"] is None and side is not None:
                        values["side"] = side
                        stored["side"] = side
                    if stored["start_position"] is None and start_position is not None:
                        values["start_position"] = start_position
                        stored["start_position"] = start_position
                    if values:
                        await conn.execute(
                            update(hl_fills)
                            .where(
                                hl_fills.c.address == address,
                                hl_fills.c.tid == tid,
                            )
                            .values(**values)
                        )

            page_tids = {int(fill["tid"]) for fill in page}
            request_complete = len(page) < self.fill_page_size
            if request_complete or (page and not (page_tids - seen_tids)):
                break
            seen_tids.update(page_tids)
            if not times:
                break
            next_start = max(times)
            if next_start < current_start:
                break
            current_start = next_start

        return sum(
            1
            for values in missing.values()
            if values["side"] is None or values["start_position"] is None
        )

    async def profile_wallet(self, address: str) -> dict[str, Any]:
        """立即计算并持久化一个已登记钱包的完整画像。"""
        normalized = normalize_address(address)
        async with self.engine.connect() as conn:
            wallet = (
                await conn.execute(
                    select(hl_wallets).where(hl_wallets.c.address == normalized)
                )
            ).mappings().first()
        if wallet is None:
            raise ValueError("钱包不在核心、重点或候选名单中")
        if wallet["excluded"]:
            raise ValueError("已排除钱包不计算画像")

        now = self.clock()
        start_at = now - PROFILE_WINDOW
        coverage = await self.sync_fills(normalized, start_at=start_at, end_at=now)
        try:
            await self._backfill_missing_fill_fields(
                normalized,
                start_at=coverage.start_at,
                end_at=coverage.end_at,
            )
        except Exception as exc:
            logger.warning(
                "钱包 %s 的旧成交字段补齐失败，将按数据不足继续计算：%s",
                normalized,
                exc,
            )
        portfolio = await self.rest.info("portfolio", budget="batch", user=normalized)
        abstraction = await self.rest.info("userAbstraction", budget="batch", user=normalized)
        clearing = await self.rest.info("clearinghouseState", budget="batch", user=normalized)
        dex_clearing_states = {
            dex: await self.rest.info(
                "clearinghouseState",
                budget="batch",
                user=normalized,
                dex=dex,
            )
            for dex in self.hip3_dexes
        }
        spot = None
        if _is_unified(abstraction):
            spot = await self.rest.info(
                "spotClearinghouseState",
                budget="batch",
                user=normalized,
            )
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(hl_fills)
                    .where(
                        hl_fills.c.address == normalized,
                        hl_fills.c.occurred_at >= coverage.start_at,
                        hl_fills.c.occurred_at <= coverage.end_at,
                    )
                    .order_by(hl_fills.c.occurred_at, hl_fills.c.tid)
                )
            ).mappings().all()
        fills = [_database_fill(row) for row in rows]
        computed = calculate_wallet_profile(
            portfolio=portfolio,
            fills=fills,
            clearing_state=clearing,
            abstraction=abstraction,
            spot_state=spot,
            coverage=FillCoverage(
                coverage.start_at,
                coverage.end_at,
                coverage.complete,
                len(fills),
            ),
            now=now,
            dex_clearing_states=dex_clearing_states,
        )
        async with self.engine.begin() as conn:
            current = (
                await conn.execute(
                    select(hl_wallets.c.profile, hl_wallets.c.excluded)
                    .where(hl_wallets.c.address == normalized)
                    .with_for_update()
                )
            ).mappings().first()
            if current is None:
                raise ValueError("钱包已从名单移除")
            if current["excluded"]:
                raise ValueError("画像计算期间钱包已被排除")
            profile = dict(current["profile"] or {})
            profile.update(computed.profile)
            await conn.execute(
                update(hl_wallets)
                .where(hl_wallets.c.address == normalized)
                .values(
                    profile=profile,
                    profile_updated_at=now,
                    style=computed.style,
                )
            )
        return {
            "address": normalized,
            "style": computed.style,
            "profile": profile,
            "profile_updated_at": now,
        }


class ProfileBatchRunner:
    """按重点优先、候选轮转策略运行可断点续跑的画像批次。"""

    def __init__(
        self,
        engine: AsyncEngine,
        profiler: Any,
        *,
        health: Any,
        clock: Clock = utcnow,
        retry_limit: int = 3,
        candidate_shards: int = 2,
        failure_ratio_threshold: float = 0.1,
        wallet_timeout_s: float = 600,
    ) -> None:
        if retry_limit <= 0 or candidate_shards <= 0 or wallet_timeout_s <= 0:
            raise ValueError("画像重试、候选分片和钱包超时必须为正数")
        if not 0 < failure_ratio_threshold <= 1:
            raise ValueError("画像失败比例阈值必须在 0 到 1 之间")
        self.engine = engine
        self.profiler = profiler
        self.health = health
        self.clock = clock
        self.retry_limit = retry_limit
        self.candidate_shards = candidate_shards
        self.failure_ratio_threshold = failure_ratio_threshold
        self.wallet_timeout_s = wallet_timeout_s

    async def run_once(self, *, batch_date: date | None = None) -> ProfileBatchResult:
        """执行或恢复一个批次，单钱包失败达到上限后继续后续地址。"""
        selected_date = batch_date or await self._resumable_date()
        addresses, next_index, failed = await self._load_or_start(selected_date)
        for index in range(next_index, len(addresses)):
            address = addresses[index]
            error: BaseException | None = None
            for attempt in range(1, self.retry_limit + 1):
                try:
                    await asyncio.wait_for(
                        self.profiler.profile_wallet(address),
                        timeout=self.wallet_timeout_s,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error = exc
                    logger.warning(
                        "钱包 %s…%s 画像第 %d/%d 次失败：%s",
                        address[:6],
                        address[-4:],
                        attempt,
                        self.retry_limit,
                        type(exc).__name__,
                    )
                else:
                    error = None
                    break
            if error is not None:
                failed.add(address)
            else:
                failed.discard(address)
            await self._save_progress(
                selected_date,
                addresses,
                index + 1,
                failed,
                phase="running",
            )

        result = ProfileBatchResult(
            processed=len(addresses),
            succeeded=len(addresses) - len(failed),
            failed=tuple(sorted(failed)),
        )
        await self._save_progress(
            selected_date,
            addresses,
            len(addresses),
            failed,
            phase="complete",
        )
        ratio = len(failed) / len(addresses) if addresses else 0
        if ratio >= self.failure_ratio_threshold:
            await self.health.failure(
                "hl_profile",
                f"画像批次失败 {len(failed)}/{len(addresses)}，失败比例 {ratio:.1%}",
            )
        else:
            await self.health.success("hl_profile")
        return result

    async def _resumable_date(self) -> date:
        """跨过零点时仍优先完成原批次，避免断点被新日期覆盖。"""
        async with self.engine.connect() as conn:
            state = await conn.scalar(
                select(hl_state.c.cursor).where(hl_state.c.name == PROFILE_BATCH_STATE)
            )
        if isinstance(state, Mapping) and state.get("phase") == "running":
            try:
                return date.fromisoformat(str(state["date"]))
            except (KeyError, ValueError):
                pass
        return self.clock().astimezone(SHANGHAI).date()

    async def initial_delay(self, initial_delay_s: float) -> float:
        """未完成批次立即续跑；首次空画像延迟；其余等待上海零点。"""
        async with self.engine.connect() as conn:
            state = await conn.scalar(
                select(hl_state.c.cursor).where(hl_state.c.name == PROFILE_BATCH_STATE)
            )
            profiles = await conn.scalar(
                select(func.count()).select_from(hl_wallets).where(
                    hl_wallets.c.profile_updated_at.is_not(None),
                    hl_wallets.c.excluded.is_(False),
                )
            )
        if isinstance(state, Mapping) and state.get("phase") == "running":
            return 0
        today = self.clock().astimezone(SHANGHAI).date().isoformat()
        if isinstance(state, Mapping) and state.get("phase") == "complete":
            if state.get("date") != today:
                return 0
            delay = seconds_until_shanghai_midnight(self.clock())
            return delay if delay > 0 else timedelta(days=1).total_seconds()
        if not profiles:
            return initial_delay_s
        return 0

    async def _load_or_start(self, selected_date: date) -> tuple[list[str], int, set[str]]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(hl_wallets.c.address, hl_wallets.c.tier).where(
                        hl_wallets.c.excluded.is_(False),
                        hl_wallets.c.tier.in_(("focus", "candidate")),
                    )
                )
            ).all()
            state = await conn.scalar(
                select(hl_state.c.cursor).where(hl_state.c.name == PROFILE_BATCH_STATE)
            )
        active = {address for address, _ in rows}
        if (
            isinstance(state, Mapping)
            and state.get("phase") == "running"
            and state.get("date") == selected_date.isoformat()
        ):
            addresses = list(state.get("addresses", []))
            return (
                addresses,
                min(int(state.get("next_index") or 0), len(addresses)),
                set(state.get("failed", [])),
            )

        focus = sorted(address for address, tier in rows if tier == "focus")
        shard = selected_date.toordinal() % self.candidate_shards
        candidates = sorted(
            address
            for address, tier in rows
            if tier == "candidate" and int(address[2:], 16) % self.candidate_shards == shard
        )
        previous_failed = []
        if isinstance(state, Mapping):
            previous_failed = [
                address
                for address in state.get("failed", [])
                if address in active and address not in focus and address not in candidates
            ]
        addresses = [*focus, *sorted(previous_failed), *candidates]
        await self._save_progress(selected_date, addresses, 0, set(), phase="running")
        return addresses, 0, set()

    async def _save_progress(
        self,
        selected_date: date,
        addresses: Sequence[str],
        next_index: int,
        failed: set[str],
        *,
        phase: str,
    ) -> None:
        now = self.clock()
        cursor = {
            "phase": phase,
            "date": selected_date.isoformat(),
            "addresses": list(addresses),
            "next_index": next_index,
            "processed": next_index,
            "failed": sorted(failed),
        }
        if phase == "complete":
            cursor["completed_at"] = now.isoformat()
        async with self.engine.begin() as conn:
            await _save_state(conn, PROFILE_BATCH_STATE, cursor, now)


async def _save_state(
    conn: AsyncConnection,
    name: str,
    cursor: Mapping[str, Any],
    updated_at: datetime,
) -> None:
    stmt = pg_insert(hl_state).values(name=name, cursor=dict(cursor), updated_at=updated_at)
    await conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
        )
    )


def _portfolio_windows(raw: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(raw, list):
        raise ValueError("portfolio 响应必须是数组")
    windows: dict[str, Mapping[str, Any]] = {}
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError("portfolio 时间窗格式无效")
        name, values = item
        if not isinstance(name, str) or not isinstance(values, Mapping):
            raise ValueError("portfolio 时间窗格式无效")
        windows[name] = values
    return windows


def _history_delta(window: Mapping[str, Any] | None, key: str) -> Decimal | None:
    history = _history(window, key)
    if len(history) < 2:
        return None
    return history[-1][1] - history[0][1]


def _history(window: Mapping[str, Any] | None, key: str) -> list[tuple[int, Decimal]]:
    if window is None:
        return []
    raw = window.get(key)
    if not isinstance(raw, list):
        return []
    result: list[tuple[int, Decimal]] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        try:
            result.append((int(item[0]), _decimal(item[1])))
        except (TypeError, ValueError):
            continue
    return sorted(result)


def _max_drawdown(
    window: Mapping[str, Any] | None,
) -> tuple[Decimal | None, Decimal | None, int, str | None]:
    pnl = _history(window, "pnlHistory")
    if len(pnl) < 2:
        return None, None, len(pnl), None

    peak_time, peak_pnl = pnl[0]
    worst = Decimal(0)
    worst_peak_time = peak_time
    for current_time, current_pnl in pnl[1:]:
        if current_pnl > peak_pnl:
            peak_time = current_time
            peak_pnl = current_pnl
            continue
        loss = peak_pnl - current_pnl
        if loss > worst:
            worst = loss
            worst_peak_time = peak_time

    aligned_account: Decimal | None = None
    for account_time, account_value in _history(window, "accountValueHistory"):
        if account_time > worst_peak_time:
            break
        aligned_account = account_value
    if aligned_account is None:
        return worst, None, len(pnl), "回撤起点前没有可对齐的账户价值"
    if aligned_account <= 0:
        return worst, None, len(pnl), "回撤起点对应的账户价值小于等于零"
    return worst, worst / aligned_account, len(pnl), None


def _window_decimal(window: Mapping[str, Any] | None, key: str) -> Decimal | None:
    if window is None or window.get(key) is None:
        return None
    return _decimal(window[key])


def _summary_decimal(clearing: Any, key: str) -> Decimal | None:
    if not isinstance(clearing, Mapping):
        raise ValueError("clearinghouseState 响应必须是对象")
    summary = clearing.get("marginSummary")
    if not isinstance(summary, Mapping):
        raise ValueError("clearinghouseState.marginSummary 必须是对象")
    value = summary.get(key)
    return None if value is None else _decimal(value)


def _configured_leverage(clearing: Any, *, dex: str = "") -> dict[str, str]:
    if not isinstance(clearing, Mapping):
        return {}
    positions = clearing.get("assetPositions")
    if not isinstance(positions, list):
        return {}
    result: dict[str, str] = {}
    for item in positions:
        position = item.get("position") if isinstance(item, Mapping) else None
        if not isinstance(position, Mapping) or not position.get("coin"):
            continue
        leverage = position.get("leverage")
        value = leverage.get("value") if isinstance(leverage, Mapping) else leverage
        parsed = _optional_decimal(value)
        if parsed is not None:
            coin = str(position["coin"])
            key = coin if not dex or ":" in coin else f"{dex}:{coin}"
            result[key] = _decimal_text(parsed) or "0"
    return result


def _configured_leverage_notionals(
    clearing: Any,
    *,
    dex: str = "",
) -> dict[str, str]:
    """返回设置杠杆仓位的当前名义价值，供 CLI 排序展示。"""
    if not isinstance(clearing, Mapping):
        return {}
    positions = clearing.get("assetPositions")
    if not isinstance(positions, list):
        return {}
    result: dict[str, str] = {}
    for item in positions:
        position = item.get("position") if isinstance(item, Mapping) else None
        if not isinstance(position, Mapping) or not position.get("coin"):
            continue
        notional = _optional_decimal(position.get("positionValue"))
        if notional is None:
            continue
        coin = str(position["coin"])
        key = coin if not dex or ":" in coin else f"{dex}:{coin}"
        result[key] = _decimal_text(abs(notional)) or "0"
    return result


def _drawdown_coverage(
    window: Mapping[str, Any] | None,
    *,
    sample_size: int,
    ratio_reason: str | None,
) -> dict[str, Any]:
    coverage = _portfolio_coverage(
        window,
        "pnlHistory",
        sample_size=sample_size,
    )
    if ratio_reason is not None and coverage["sufficient"]:
        coverage["sufficient"] = False
        coverage["reason"] = ratio_reason
    return coverage


def _portfolio_coverage(
    window: Mapping[str, Any] | None,
    key: str,
    *,
    expected_days: int | None = None,
    sample_size: int | None = None,
) -> dict[str, Any]:
    history = _history(window, key)
    count = len(history) if sample_size is None else sample_size
    start = datetime.fromtimestamp(history[0][0] / 1000, tz=timezone.utc) if history else None
    end = datetime.fromtimestamp(history[-1][0] / 1000, tz=timezone.utc) if history else None
    complete = None
    if expected_days is not None and start is not None and end is not None:
        complete = (end - start) >= timedelta(days=expected_days - 1)
    return _coverage(
        start,
        end,
        count,
        sufficient=len(history) >= 2,
        complete=complete,
        reason=None if len(history) >= 2 else "历史点不足 2 个",
    )


def _coverage(
    start_at: datetime | None,
    end_at: datetime | None,
    sample_size: int,
    *,
    sufficient: bool = True,
    complete: bool | None = True,
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "start_at": start_at.isoformat() if start_at is not None else None,
        "end_at": end_at.isoformat() if end_at is not None else None,
        "sample_size": sample_size,
        "complete": complete,
        "sufficient": sufficient,
        "reason": reason,
    }


def _is_closing_fill(fill: Mapping[str, Any]) -> bool:
    direction = str(fill.get("dir") or "")
    closed = _decimal(fill.get("closedPnl", fill.get("closed_pnl", 0)))
    return direction.startswith("Close ") or " > " in direction or closed != 0


def _fill_row(address: str, fill: Mapping[str, Any]) -> dict[str, Any]:
    try:
        occurred_at = datetime.fromtimestamp(int(fill["time"]) / 1000, tz=timezone.utc)
        tid = int(fill["tid"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("成交记录缺少有效 time 或 tid") from exc
    start_position = _optional_decimal(fill.get("startPosition"))
    side = str(fill.get("side") or "") or None
    return {
        "address": normalize_address(address),
        "tid": tid,
        "coin": str(fill["coin"]),
        "dir": str(fill.get("dir") or ""),
        "side": side,
        "start_position": start_position,
        "px": _decimal(fill.get("px") or 0),
        "sz": _decimal(fill.get("sz") or 0),
        "closed_pnl": _decimal(fill.get("closedPnl") or 0),
        "fee": _decimal(fill.get("fee") or 0),
        "crossed": bool(fill.get("crossed")),
        "occurred_at": occurred_at,
        "hash": str(fill.get("hash") or ""),
        "is_twap": bool(fill.get("twapId") is not None),
        "liquidation": (
            dict(fill["liquidation"])
            if isinstance(fill.get("liquidation"), Mapping)
            else None
        ),
    }


def _database_fill(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "coin": row["coin"],
        "px": row["px"],
        "sz": row["sz"],
        "side": row.get("side"),
        "time": _timestamp_ms(row),
        "startPosition": row.get("start_position"),
        "dir": row["dir"],
        "closedPnl": row["closed_pnl"],
        "crossed": row["crossed"],
        "tid": row["tid"],
    }


def _timestamp_ms(fill: Mapping[str, Any]) -> int:
    value = fill.get("occurred_at")
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return 0


def _is_unified(value: Any) -> bool:
    if isinstance(value, Mapping):
        value = value.get("type") or value.get("userAbstraction")
    return value in {"unifiedAccount", "portfolioMargin"}


def _decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"画像数值无效：{value}") from exc
    if not parsed.is_finite():
        raise ValueError(f"画像数值无效：{value}")
    return parsed


def _optional_decimal(value: Any) -> Decimal | None:
    return None if value is None else _decimal(value)


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
