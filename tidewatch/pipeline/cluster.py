"""事件聚簇（系统设计 6.4）

- news ↔ news：窗口内与簇首条事件的标题相似度 ≥ 阈值；双方资产都非空时还需有交集
- news ↔ exchange：快讯标题同时含交易所名与上币/下架类关键词，且标题中出现公告涉及的任一币种代码
- exchange ↔ exchange：不合并（同一公告的重复已由 dedupe_key 去除）
- hyperliquid：按（地址、币种、事件族）聚簇，使用独立 30 分钟窗口
- 其他类别：每个事件单独成簇
窗口从簇的 first_seen_at 起算；候选全部从数据库查询，重启无需恢复内存状态。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.config import ExchangeRulesCfg, PipelineCfg
from tidewatch.models import Event, max_level
from tidewatch.pipeline.normalize import contains_symbol, title_similarity
from tidewatch.pipeline.rules.keywords import text_contains
from tidewatch.store.tables import event_clusters, events

_MATCHABLE = ("news", "exchange")
_HYPERLIQUID_FAMILIES = {
    "hl_open": "position_add",
    "hl_increase": "position_add",
    "hl_decrease": "position_exit",
    "hl_close": "position_exit",
    "hl_liq_risk": "liquidation_risk",
    "hl_liquidated": "liquidated",
    "hl_new_whale": "new_whale",
}


def _require_saved(event: Event) -> None:
    if event.id is None:
        raise ValueError("事件尚未入库")


@dataclass
class ClusterState:
    """聚簇后簇的最新状态"""

    id: int
    category: str
    max_level: str
    committed_level: str | None
    delivered_level: str | None
    member_count: int
    source_count: int


class Clusterer:
    def __init__(
        self,
        pipeline_cfg: PipelineCfg,
        exchange_cfg: ExchangeRulesCfg,
        *,
        hyperliquid_window_s: float = 1800,
    ) -> None:
        self._window = timedelta(seconds=pipeline_cfg.cluster_window_s)
        self._hyperliquid_window = timedelta(seconds=hyperliquid_window_s)
        self._threshold = pipeline_cfg.title_similarity
        self._exchange_names = list(exchange_cfg.exchange_names)
        self._listing_keywords = list(exchange_cfg.listing_keywords)

    async def assign(self, conn: AsyncConnection, event: Event, now: datetime) -> ClusterState:
        """把已落库并分级的事件归入已有簇或新建簇"""
        cluster_id = None
        if event.category in _MATCHABLE:
            cluster_id = await self._find(conn, event, now - self._window)
        elif event.category == "hyperliquid":
            cluster_key = self._cluster_key(event)
            if cluster_key is not None:
                cluster_id = await self._find_hyperliquid(conn, cluster_key, now - self._hyperliquid_window)
        if cluster_id is None:
            return await self.create_single(conn, event, now)
        return await self._join(conn, cluster_id, event, now)

    async def create_single(self, conn: AsyncConnection, event: Event, now: datetime) -> ClusterState:
        """为事件新建一个只含它自己的簇；事件必须已入库"""
        _require_saved(event)  # 先校验，避免报错前已写入孤立的簇记录
        cluster_id = (
            await conn.execute(
                insert(event_clusters)
                .values(
                    category=event.category,
                    cluster_key=self._cluster_key(event),
                    first_event_id=event.id,
                    first_seen_at=now,
                    last_seen_at=now,
                    max_level=event.final_level,
                )
                .returning(event_clusters.c.id)
            )
        ).scalar_one()
        await self._attach(conn, event, cluster_id)
        return await self._state(conn, cluster_id)

    async def _find(self, conn: AsyncConnection, event: Event, since: datetime) -> int | None:
        stmt = (
            select(event_clusters.c.id, event_clusters.c.category, events.c.title, events.c.assets)
            .select_from(event_clusters.join(events, events.c.id == event_clusters.c.first_event_id))
            .where(event_clusters.c.category.in_(_MATCHABLE), event_clusters.c.first_seen_at >= since)
            # first_seen_at 相同时按 id 倒序，保证并入哪个簇的结果是确定的
            .order_by(event_clusters.c.first_seen_at.desc(), event_clusters.c.id.desc())
        )
        for row in (await conn.execute(stmt)).all():
            if self._matches(event, row.category, row.title, row.assets):
                return row.id
        return None

    @staticmethod
    async def _find_hyperliquid(conn: AsyncConnection, cluster_key: str, since: datetime) -> int | None:
        return (
            await conn.execute(
                select(event_clusters.c.id)
                .where(
                    event_clusters.c.category == "hyperliquid",
                    event_clusters.c.cluster_key == cluster_key,
                    event_clusters.c.first_seen_at >= since,
                )
                .order_by(event_clusters.c.first_seen_at.desc(), event_clusters.c.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    @staticmethod
    def _cluster_key(event: Event) -> str | None:
        if event.category != "hyperliquid":
            return None
        family = _HYPERLIQUID_FAMILIES.get(event.event_type)
        address = event.raw.get("address")
        coin = event.raw.get("coin") or next(iter(event.assets), None)
        if family is None or not isinstance(address, str) or not address or not isinstance(coin, str) or not coin:
            return None
        return json.dumps([address.lower(), coin, family], ensure_ascii=False, separators=(",", ":"))

    def _matches(self, event: Event, category: str, title: str, assets: list[str]) -> bool:
        if event.category == "news" and category == "news":
            if event.assets and assets and not set(event.assets) & set(assets):
                return False
            return title_similarity(event.title, title) >= self._threshold
        if {event.category, category} == {"news", "exchange"}:
            if event.category == "news":
                news_title, symbols = event.title, assets
            else:
                news_title, symbols = title, event.assets
            return self._is_listing_news(news_title) and any(contains_symbol(news_title, sym) for sym in symbols)
        return False

    def _is_listing_news(self, title: str) -> bool:
        # 英文关键词按独立单词匹配（避免 list 命中 blacklist）
        return any(text_contains(title, name) for name in self._exchange_names) and any(
            text_contains(title, word) for word in self._listing_keywords
        )

    async def _join(self, conn: AsyncConnection, cluster_id: int, event: Event, now: datetime) -> ClusterState:
        await self._attach(conn, event, cluster_id)
        counts = (
            await conn.execute(
                select(func.count(), func.count(func.distinct(events.c.source))).where(events.c.cluster_id == cluster_id)
            )
        ).one()
        current = (
            await conn.execute(select(event_clusters.c.max_level).where(event_clusters.c.id == cluster_id))
        ).scalar_one()
        await conn.execute(
            update(event_clusters)
            .where(event_clusters.c.id == cluster_id)
            .values(
                last_seen_at=now,
                member_count=counts[0],
                source_count=counts[1],
                max_level=max_level(current, event.final_level),
            )
        )
        return await self._state(conn, cluster_id)

    @staticmethod
    async def _attach(conn: AsyncConnection, event: Event, cluster_id: int) -> None:
        _require_saved(event)
        await conn.execute(update(events).where(events.c.id == event.id).values(cluster_id=cluster_id))
        event.cluster_id = cluster_id

    @staticmethod
    async def _state(conn: AsyncConnection, cluster_id: int) -> ClusterState:
        row = (
            await conn.execute(
                select(
                    event_clusters.c.id,
                    event_clusters.c.category,
                    event_clusters.c.max_level,
                    event_clusters.c.committed_level,
                    event_clusters.c.delivered_level,
                    event_clusters.c.member_count,
                    event_clusters.c.source_count,
                ).where(event_clusters.c.id == cluster_id)
            )
        ).one()
        return ClusterState(**row._mapping)
