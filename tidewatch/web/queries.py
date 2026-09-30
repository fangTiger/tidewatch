"""网站查询：时间线（以簇为单位）与状态页"""

from __future__ import annotations

from datetime import datetime, time as dtime
from typing import Any

from sqlalchemy import case, exists, func, select, true
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import LOCAL_TZ, format_local
from tidewatch.notify.base import source_name
from tidewatch.pipeline.decision import FreshnessLookup
from tidewatch.store.tables import collector_state, event_clusters, events, push_log

LEVELS = ("S", "A", "B", "C")
STATUS_TEXT = {"normal": "正常", "abnormal": "异常", "unknown": "未知"}
PUSH_STATUSES = ("ok", "dry_run", "failed", "expired", "pending", "superseded")

_RANK = case({"S": 3, "A": 2, "B": 1}, value=events.c.final_level, else_=0)


def _cluster_select():
    """簇 + 代表事件（LATERAL 子查询：已推送取 rep_event_id，否则取等级最高、最早的一条）"""
    rep = (
        select(
            events.c.id, events.c.title, events.c.url, events.c.source,
            events.c.occurred_at, events.c.collected_at, events.c.assets,
        )
        .where(events.c.cluster_id == event_clusters.c.id)
        .order_by(
            (events.c.id == event_clusters.c.rep_event_id).desc().nulls_last(),
            _RANK.desc(),
            events.c.occurred_at,
            events.c.id,
        )
        .limit(1)
        .lateral("rep")
    )
    stmt = select(
        event_clusters.c.id.label("cluster_id"),
        event_clusters.c.category,
        event_clusters.c.max_level,
        event_clusters.c.committed_level,
        event_clusters.c.source_count,
        rep.c.id.label("event_id"),
        rep.c.title,
        rep.c.url,
        rep.c.source,
        rep.c.occurred_at,
        rep.c.collected_at,
        rep.c.assets,
    ).select_from(event_clusters.join(rep, true()))
    return stmt, rep


def _to_dict(row: Any, freshness: FreshnessLookup) -> dict[str, Any]:
    max_age = freshness(row.source)
    stale = max_age is not None and (row.collected_at - row.occurred_at) > max_age
    return {
        "cluster_id": row.cluster_id,
        "event_id": row.event_id,
        "category": row.category,
        "level": row.max_level,
        "title": row.title,
        "url": row.url,
        "source": row.source,
        "source_name": source_name(row.source),
        "source_count": row.source_count,
        "assets": list(row.assets),
        "occurred_at": row.occurred_at.isoformat(),
        "time_local": format_local(row.occurred_at, "%m-%d %H:%M:%S"),
        "pushed": row.committed_level is not None,
        "backfill": row.committed_level is None and stale,
    }


async def timeline(
    engine: AsyncEngine,
    *,
    freshness: FreshnessLookup,
    category: str | None = None,
    level: str | None = None,
    asset: str | None = None,
    include_c: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """首页时间线：按代表事件的发生时间倒序"""
    stmt, rep = _cluster_select()
    if category:
        stmt = stmt.where(event_clusters.c.category == category)
    if level in LEVELS:
        stmt = stmt.where(event_clusters.c.max_level == level)
    elif not include_c:
        stmt = stmt.where(event_clusters.c.max_level != "C")
    if asset:
        stmt = stmt.where(
            exists().where(events.c.cluster_id == event_clusters.c.id, events.c.assets.contains([asset]))
        )
    stmt = stmt.order_by(rep.c.occurred_at.desc()).limit(limit)
    async with engine.connect() as conn:
        rows = (await conn.execute(stmt)).all()
    return [_to_dict(row, freshness) for row in rows]


async def cluster_rows(engine: AsyncEngine, cluster_ids: list[int], *, freshness: FreshnessLookup) -> list[dict[str, Any]]:
    """指定簇的时间线行（用于 SSE 增量更新），按代表事件发生时间倒序"""
    if not cluster_ids:
        return []
    stmt, rep = _cluster_select()
    stmt = stmt.where(event_clusters.c.id.in_(cluster_ids)).order_by(rep.c.occurred_at.desc())
    async with engine.connect() as conn:
        rows = (await conn.execute(stmt)).all()
    return [_to_dict(row, freshness) for row in rows]


def _local_time(value: datetime | None) -> str:
    if value is None:
        return "—"
    return format_local(value, "%H:%M:%S")


async def collector_status(engine: AsyncEngine) -> list[dict[str, Any]]:
    """状态页：各采集器的健康状态"""
    async with engine.connect() as conn:
        rows = (await conn.execute(select(collector_state).order_by(collector_state.c.name))).mappings().all()
    return [
        {
            "name": row["name"],
            "source_name": source_name(row["name"]),
            "status": row["status"],
            "status_text": STATUS_TEXT.get(row["status"], row["status"]),
            "mode": row["mode"],
            "mode_text": "降级" if row["mode"] == "degraded" else "正常",
            "last_success": _local_time(row["last_success_at"]),
            "last_event": _local_time(row["last_event_at"]),
            "consecutive_failures": row["consecutive_failures"],
            "last_error": row["last_error"] or "",
        }
        for row in rows
    ]


async def push_stats(engine: AsyncEngine, now: datetime) -> dict[str, int]:
    """状态页：当天（上海时间）按状态统计的推送数"""
    day_start = datetime.combine(now.astimezone(LOCAL_TZ).date(), dtime.min, tzinfo=LOCAL_TZ)
    stmt = (
        select(push_log.c.status, func.count())
        .where(push_log.c.created_at >= day_start)
        .group_by(push_log.c.status)
    )
    async with engine.connect() as conn:
        counts = dict((await conn.execute(stmt)).all())
    return {status: int(counts.get(status, 0)) for status in PUSH_STATUSES}
