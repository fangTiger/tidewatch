"""实时更新：把流水线与健康监控的回调转成 SSE 消息"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.pipeline.decision import freshness_from_settings
from tidewatch.pipeline.sink import IngestOutcome
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.queries import cluster_rows, market_tickers, whale_detail, whale_net_positions

logger = logging.getLogger(__name__)

# 刷新失败（如数据库故障）后的退避范围，避免故障期间空转
INITIAL_RETRY_DELAY_S = 1
MAX_RETRY_DELAY_S = 30


class LiveUpdates:
    """把流水线回调汇聚为可供网页订阅的实时 SSE 更新。"""

    def __init__(
        self,
        engine: AsyncEngine,
        broadcaster: Broadcaster,
        settings: Any,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        clock: Clock = utcnow,
    ) -> None:
        self._engine = engine
        self._broadcaster = broadcaster
        self._freshness = freshness_from_settings(settings)
        self._sleep = sleep
        self._monotonic = monotonic
        self._clock = clock
        self._ticker_symbols = list(settings.hyperliquid.ticker_symbols)
        self._net_stale_after_s = settings.hyperliquid.health.hl_rest.stale_after_s
        self._last_market_publish_at: float | None = None
        self._market_lock = asyncio.Lock()
        self._pending: asyncio.Queue[list[int]] = asyncio.Queue()

    def on_ingest(self, outcome: IngestOutcome) -> None:
        """EventSink 提交后的同步回调：先发 event.created，簇行查询交给后台协程"""
        self._broadcaster.publish("event.created", {"event_ids": outcome.event_ids})
        self._pending.put_nowait(sorted(set(outcome.cluster_ids)))

    def on_health(self, payload: dict[str, Any]) -> None:
        """把健康监控提交后的状态变化立即广播给订阅者。"""
        self._broadcaster.publish("health.changed", payload)

    async def on_market(self, mids: Mapping[str, Decimal]) -> None:
        """以两秒间隔广播配置币种行情；串行调用时自然合并到运行时的单槽最新值队列。"""
        async with self._market_lock:
            if self._last_market_publish_at is not None:
                elapsed = self._monotonic() - self._last_market_publish_at
                if elapsed < 2:
                    await self._sleep(2 - elapsed)
            self._last_market_publish_at = self._monotonic()
            self._broadcaster.publish(
                "market.updated",
                {
                    "enabled": True,
                    "tickers": market_tickers(
                        self._ticker_symbols,
                        dict(mids),
                        enabled=True,
                        connected=True,
                    ),
                },
            )

    async def on_position(self, address: str, mids: Mapping[str, Decimal]) -> None:
        """仓位事务提交后读取已提交视图，再广播详情仓位与全局净仓位。"""
        detail = await whale_detail(self._engine, address, mids=dict(mids))
        if detail is not None:
            self._broadcaster.publish(
                "position.updated",
                {"address": address, "positions": detail["positions"]},
            )
        net_positions = await whale_net_positions(
            self._engine,
            mids=dict(mids),
            now=self._clock(),
            stale_after_s=self._net_stale_after_s,
        )
        self._broadcaster.publish(
            "whales.net_position.updated",
            {
                "net_positions": net_positions,
                "net_updated_at": max((row["updated_at"] for row in net_positions), default=None),
            },
        )

    async def process_once(self) -> None:
        """处理队列中所有待刷新的簇；查询失败时把这批簇放回队列再抛出，下次重试时不丢更新"""
        ids: set[int] = set()
        while not self._pending.empty():
            ids.update(self._pending.get_nowait())
        batch = sorted(ids)
        try:
            rows = await cluster_rows(self._engine, batch, freshness=self._freshness)
        except Exception:
            if batch:
                self._pending.put_nowait(batch)
            raise
        for row in rows:
            self._broadcaster.publish("cluster.updated", row)

    async def run(self) -> None:
        """持续刷新待处理簇；失败时指数退避，任一次成功后重置等待时间。"""
        retry_delay = INITIAL_RETRY_DELAY_S
        while True:
            self._pending.put_nowait(await self._pending.get())  # 等待至少一批
            try:
                await self.process_once()
            except Exception:
                logger.exception("实时更新失败，%s 秒后重试", retry_delay)
                await self._sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY_S)
            else:
                retry_delay = INITIAL_RETRY_DELAY_S
