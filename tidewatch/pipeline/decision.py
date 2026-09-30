"""以簇为单位的推送决策（系统设计 6.7）"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.models import max_level
from tidewatch.store.repo import OPEN_STATUSES
from tidewatch.store.tables import event_clusters, events, push_log

# 按事件来源返回新鲜度上限（None 表示不限）
FreshnessLookup = Callable[[str], timedelta | None]

_DEFAULT_MAX_AGE = timedelta(minutes=30)


def freshness_from_settings(settings) -> FreshnessLookup:
    """从采集器配置生成新鲜度查询；未配置的来源默认 30 分钟"""

    def lookup(source: str) -> timedelta | None:
        cfg = settings.collectors.get(source)
        if cfg is None:
            return _DEFAULT_MAX_AGE
        return None if cfg.max_push_age_s is None else timedelta(seconds=cfg.max_push_age_s)

    return lookup


@dataclass
class PushPlan:
    """一次已安排（写入 push_log）的推送"""

    push_id: int
    cluster_id: int
    event_id: int
    level: str
    kind: str  # event / upgrade
    superseded_ids: list[int] = field(default_factory=list)


class PushDecider:
    def __init__(self, freshness: FreshnessLookup, retry_max: timedelta) -> None:
        self._freshness = freshness
        self._retry_max = retry_max

    def is_fresh(self, source: str, occurred_at: datetime, now: datetime) -> bool:
        max_age = self._freshness(source)
        return max_age is None or now - occurred_at <= max_age

    async def decide(self, conn: AsyncConnection, cluster_id: int, now: datetime) -> PushPlan | None:
        """根据簇当前状态决定是否安排推送；安排时写入 push_log 并更新 committed_level"""
        cluster = (
            await conn.execute(
                select(event_clusters.c.committed_level, event_clusters.c.delivered_level).where(
                    event_clusters.c.id == cluster_id
                )
            )
        ).one()
        members = (
            await conn.execute(
                select(events.c.id, events.c.source, events.c.occurred_at, events.c.final_level)
                .where(events.c.cluster_id == cluster_id)
                .order_by(events.c.occurred_at, events.c.id)
            )
        ).all()
        fresh = [m for m in members if self.is_fresh(m.source, m.occurred_at, now)]
        top = max_level(*(m.final_level for m in fresh))
        if top not in ("S", "A"):
            return None

        superseded: list[int] = []
        if cluster.committed_level is None:
            kind = "event"
        elif top == "S" and cluster.committed_level == "A":
            result = await conn.execute(
                update(push_log)
                .where(push_log.c.cluster_id == cluster_id, push_log.c.status.in_(OPEN_STATUSES))
                .values(status="superseded")
                .returning(push_log.c.id)
            )
            superseded = list(result.scalars().all())
            kind = "upgrade" if cluster.delivered_level == "A" else "event"
        else:
            return None

        rep = next(m for m in fresh if m.final_level == top)
        retry_until = now + self._retry_max
        max_age = self._freshness(rep.source)
        if max_age is not None:
            retry_until = min(retry_until, rep.occurred_at + max_age)
        push_id = (
            await conn.execute(
                insert(push_log)
                .values(
                    cluster_id=cluster_id,
                    event_id=rep.id,
                    level=top,
                    kind=kind,
                    status="pending",
                    retry_until=retry_until,
                    created_at=now,
                )
                .returning(push_log.c.id)
            )
        ).scalar_one()
        await conn.execute(
            update(event_clusters)
            .where(event_clusters.c.id == cluster_id)
            .values(committed_level=top, rep_event_id=rep.id)
        )
        return PushPlan(push_id, cluster_id, rep.id, top, kind, superseded)
