"""Hyperliquid 子系统运行时与 M2 名单调度。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.collectors.base import TokenBucket
from tidewatch.collectors.http import make_client
from tidewatch.hyperliquid.client import HyperliquidRestClient
from tidewatch.hyperliquid.importer import AliasCache, LeaderboardImporter
from tidewatch.hyperliquid.leaderboard import HYPURRSCAN_ALIASES_URL, LEADERBOARD_URL, LeaderboardSource
from tidewatch.hyperliquid.registry import WalletRegistry
from tidewatch.hyperliquid.tiers import TierRecalculator
from tidewatch.hyperliquid.ws import HyperliquidWsManager
from tidewatch.store.tables import hl_state


HEALTH_SOURCES = ("hl_rest", "hl_trades_ws", "hl_core_ws", "hl_leaderboard", "hl_profile")
SHANGHAI = ZoneInfo("Asia/Shanghai")
REPO_ROOT = Path(__file__).resolve().parents[2]

logger = logging.getLogger(__name__)


def seconds_until_shanghai_8(now: datetime) -> float:
    """返回从当前时刻到下一个上海时间 08:00 的秒数。"""
    local = now.astimezone(SHANGHAI)
    target = local.replace(hour=8, minute=0, second=0, microsecond=0)
    if target < local:
        target += timedelta(days=1)
    return (target - local).total_seconds()


class HyperliquidRuntime:
    """装配 Hyperliquid 客户端、名单服务和受守护后台任务。"""

    def __init__(
        self,
        settings: Any,
        health: Any,
        *,
        engine: AsyncEngine,
        proxy_url: str | None = None,
        clock: Clock = utcnow,
        data_dir: Path | None = None,
    ) -> None:
        self.settings = settings
        self.health = health
        self.engine = engine
        self.clock = clock
        self.monitoring_bucket = TokenBucket(
            settings.monitor_budget_per_min / 60,
            capacity=settings.monitor_budget_per_min,
        )
        self.batch_bucket = TokenBucket(
            settings.batch_budget_per_min / 60,
            capacity=settings.batch_budget_per_min,
        )
        self.rest = HyperliquidRestClient(
            self.monitoring_bucket,
            self.batch_bucket,
            proxy_mode=settings.proxy,
            proxy_url=proxy_url,
            health=health,
            url=settings.rest_url,
        )
        self.ws = HyperliquidWsManager(
            proxy_mode=settings.proxy,
            proxy_url=proxy_url,
            health=health,
            url=settings.ws_url,
            ping_interval_s=settings.frequencies.ws_ping_interval_s,
        )
        self.http = make_client(settings.proxy, proxy_url, timeout=60)
        self.registry = WalletRegistry(
            engine,
            configured_core=settings.core_wallets,
            core_limit=settings.tiers.core_limit,
            focus_limit=settings.tiers.focus_limit,
            clock=clock,
        )
        leaderboard = LeaderboardSource(
            client=self.http,
            url=getattr(settings, "leaderboard_url", LEADERBOARD_URL),
            protocol_addresses=settings.protocol_excluded_addresses,
        )
        labels = AliasCache(
            (data_dir or REPO_ROOT / "data") / "labels",
            client=self.http,
            url=getattr(settings, "aliases_url", HYPURRSCAN_ALIASES_URL),
            refresh_interval=timedelta(seconds=settings.frequencies.label_refresh_interval_s),
            clock=clock,
        )
        self.importer = LeaderboardImporter(
            self.registry,
            leaderboard,
            labels,
            health=health,
            clock=clock,
            candidate_limit=settings.tiers.candidate_limit,
            minimum_value=Decimal(str(settings.events.discovery_account_value_usd)),
        )
        self.tiers = TierRecalculator(
            engine,
            self.rest,
            minimum_notional=Decimal(str(settings.events.discovery_account_value_usd)),
            focus_limit=settings.tiers.focus_limit,
            clock=clock,
        )
        self._tier_lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._resources_closed = False

    def register_health(self) -> None:
        """登记五个互相独立的健康子源。"""
        stale = self.settings.health
        for name in HEALTH_SOURCES:
            self.health.register(name, getattr(stale, name).stale_after_s)

    async def start(self) -> None:
        """先同步配置核心，再启动全部受关键任务守护的循环。"""
        logger.info(
            "Hyperliquid 使用 %s 代理模式；"
            "上游按出口 IP 限流，切换代理节点会改变限流出口 IP",
            self.settings.proxy,
        )
        await self.registry.sync_configured_core()
        self._stop.clear()
        specs: tuple[tuple[str, Callable[[], Awaitable[None]]], ...] = (
            ("hl-rest", self.run_rest),
            ("hl-trades-ws", self.run_trades_ws),
            ("hl-core-ws", self.run_core_ws),
            ("hl-leaderboard", self.run_leaderboard),
            ("hl-tier-recompute", self.run_tier_recompute),
            ("hl-profile", self.run_profile),
            ("hl-market-data", self.run_market_data),
        )
        self._tasks = [asyncio.create_task(target(), name=name) for name, target in specs]

    async def _idle(self) -> None:
        await self._stop.wait()

    async def _idle_health(self, source: str) -> None:
        """尚未交付的 M3/M5 子源继续沿用 M1 空闲存活上报。"""
        interval = getattr(self.settings.health, source).stale_after_s / 2
        while not self._stop.is_set():
            await self.health.success(source)
            if await self._wait_or_stop(interval):
                return

    async def run_rest(self) -> None:
        await self._idle_health("hl_rest")

    async def run_trades_ws(self) -> None:
        await self._idle_health("hl_trades_ws")

    async def run_core_ws(self) -> None:
        await self._idle_health("hl_core_ws")

    async def run_leaderboard(self) -> None:
        """空候选池立即导入；其后每 12 小时导入并紧接一次分层重算。"""
        initial_delay = await self._leaderboard_initial_delay()
        if initial_delay > 0 and await self._wait_or_stop(initial_delay):
            return
        while not self._stop.is_set():
            try:
                await self.importer.import_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Hyperliquid 排行榜导入失败")
                retry_delay = min(300, self.settings.frequencies.leaderboard_interval_s)
                if await self._wait_or_stop(retry_delay):
                    return
                continue
            try:
                async with self._tier_lock:
                    await self.tiers.recompute()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Hyperliquid 排行榜导入后的分层重算失败")
                retry_delay = min(300, self.settings.frequencies.leaderboard_interval_s)
                if await self._wait_or_stop(retry_delay):
                    return
                try:
                    async with self._tier_lock:
                        await self.tiers.recompute()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Hyperliquid 导入后的分层重算重试失败")
            if await self._wait_or_stop(self.settings.frequencies.leaderboard_interval_s):
                return

    async def run_tier_recompute(self) -> None:
        """每天上海时间 08:00 运行与导入后相同的分层规则。"""
        last_run_date = None
        retrying = False
        while not self._stop.is_set():
            now = self.clock()
            if retrying:
                delay = min(300, self.settings.frequencies.tier_recompute_interval_s)
            else:
                delay = seconds_until_shanghai_8(now)
                if delay == 0 and last_run_date == now.astimezone(SHANGHAI).date():
                    delay = self.settings.frequencies.tier_recompute_interval_s
            if await self._wait_or_stop(delay):
                return
            try:
                async with self._tier_lock:
                    await self.tiers.recompute()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Hyperliquid 每日分层重算失败")
                retrying = True
            else:
                retrying = False
                last_run_date = self.clock().astimezone(SHANGHAI).date()

    async def _leaderboard_initial_delay(self) -> float:
        """按上次完成游标恢复十二小时节拍；空候选池仍立即导入。"""
        if not await self.registry.has_candidates():
            return 0
        async with self.engine.connect() as conn:
            cursor = await conn.scalar(
                select(hl_state.c.cursor).where(hl_state.c.name == "leaderboard_import")
            )
        if not isinstance(cursor, dict):
            return self.settings.frequencies.leaderboard_interval_s
        if cursor.get("phase") != "complete":
            return 0
        try:
            completed_at = datetime.fromisoformat(cursor["completed_at"])
            elapsed = (self.clock() - completed_at).total_seconds()
        except (KeyError, TypeError, ValueError):
            return self.settings.frequencies.leaderboard_interval_s
        return max(0, self.settings.frequencies.leaderboard_interval_s - max(0, elapsed))

    async def run_profile(self) -> None:
        await self._idle_health("hl_profile")

    async def run_market_data(self) -> None:
        await self._idle()

    async def _wait_or_stop(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return True

    def background_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(self._tasks)

    async def stop(self) -> None:
        """停止新调度，取消在途外部请求并释放连接。"""
        self._stop.set()
        await self.ws.aclose()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.close_resources()

    async def close_resources(self) -> None:
        """关闭 HTTP 资源；对测试与部分启动可重复调用。"""
        if self._resources_closed:
            return
        self._resources_closed = True
        await self.rest.aclose()
        await self.http.aclose()
