"""实时更新：把流水线与健康监控的回调转成 SSE 消息"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.pipeline.decision import freshness_from_settings
from tidewatch.pipeline.sink import IngestOutcome
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.queries import cluster_rows

logger = logging.getLogger(__name__)


class LiveUpdates:
    def __init__(self, engine: AsyncEngine, broadcaster: Broadcaster, settings: Any) -> None:
        self._engine = engine
        self._broadcaster = broadcaster
        self._freshness = freshness_from_settings(settings)
        self._pending: asyncio.Queue[list[int]] = asyncio.Queue()

    def on_ingest(self, outcome: IngestOutcome) -> None:
        """EventSink 提交后的同步回调：先发 event.created，簇行查询交给后台协程"""
        self._broadcaster.publish("event.created", {"event_ids": outcome.event_ids})
        self._pending.put_nowait(sorted(set(outcome.cluster_ids)))

    def on_health(self, payload: dict[str, Any]) -> None:
        self._broadcaster.publish("health.changed", payload)

    async def process_once(self) -> None:
        """处理队列中所有待刷新的簇"""
        ids: set[int] = set()
        while not self._pending.empty():
            ids.update(self._pending.get_nowait())
        for row in await cluster_rows(self._engine, sorted(ids), freshness=self._freshness):
            self._broadcaster.publish("cluster.updated", row)

    async def run(self) -> None:
        while True:
            self._pending.put_nowait(await self._pending.get())  # 等待至少一批
            try:
                await self.process_once()
            except Exception:
                logger.exception("实时更新失败")
