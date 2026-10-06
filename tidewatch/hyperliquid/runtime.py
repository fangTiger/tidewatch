"""Hyperliquid M1 运行时骨架。

本阶段只负责资源装配、健康子源登记和长期任务守护；业务订阅与调度在后续里程碑接入。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from tidewatch.collectors.base import TokenBucket
from tidewatch.hyperliquid.client import HyperliquidRestClient
from tidewatch.hyperliquid.ws import HyperliquidWsManager


HEALTH_SOURCES = (
    "hl_rest",
    "hl_trades_ws",
    "hl_core_ws",
    "hl_leaderboard",
    "hl_profile",
)

logger = logging.getLogger(__name__)


class HyperliquidRuntime:
    """可启动、可取消的 Hyperliquid 子系统骨架。"""

    def __init__(self, settings: Any, health: Any, *, proxy_url: str | None = None) -> None:
        self.settings = settings
        self.health = health
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
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    def register_health(self) -> None:
        """登记五个互相独立的健康子源。"""
        stale = self.settings.health
        for name in HEALTH_SOURCES:
            self.health.register(name, getattr(stale, name).stale_after_s)

    async def start(self) -> None:
        """启动 M1 骨架任务；启动本身不连接外部网络。"""
        logger.info(
            "Hyperliquid 使用 %s 代理模式；上游按出口 IP 限流，切换代理节点会改变限流出口 IP",
            self.settings.proxy,
        )
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
        """M1 无业务订阅时上报骨架循环存活，避免把空闲误判为数据源故障。"""
        interval = getattr(self.settings.health, source).stale_after_s / 2
        while not self._stop.is_set():
            await self.health.success(source)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def run_rest(self) -> None:
        await self._idle_health("hl_rest")

    async def run_trades_ws(self) -> None:
        await self._idle_health("hl_trades_ws")

    async def run_core_ws(self) -> None:
        await self._idle_health("hl_core_ws")

    async def run_leaderboard(self) -> None:
        await self._idle_health("hl_leaderboard")

    async def run_tier_recompute(self) -> None:
        await self._idle()

    async def run_profile(self) -> None:
        await self._idle_health("hl_profile")

    async def run_market_data(self) -> None:
        await self._idle()

    def background_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(self._tasks)

    async def stop(self) -> None:
        """停止接收新工作并等待所有骨架任务退出。"""
        self._stop.set()
        await self.ws.aclose()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.rest.aclose()
