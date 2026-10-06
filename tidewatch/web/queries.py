"""网站查询：时间线（以簇为单位）与状态页"""

from __future__ import annotations

from datetime import datetime, time as dtime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import case, exists, func, literal_column, select, true
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import LOCAL_TZ, format_local, utcnow
from tidewatch.hyperliquid.registry import normalize_address
from tidewatch.notify.base import source_name
from tidewatch.pipeline.decision import FreshnessLookup
from tidewatch.store.tables import (
    collector_state,
    event_clusters,
    events,
    hl_fills,
    hl_positions,
    hl_wallets,
    push_log,
)
from tidewatch.urls import safe_http_url

LEVELS = ("S", "A", "B", "C")
STATUS_TEXT = {"normal": "正常", "abnormal": "异常", "unknown": "未知"}
PUSH_STATUSES = ("ok", "dry_run", "failed", "expired", "pending", "superseded")

_RANK = case({"S": 3, "A": 2, "B": 1}, value=events.c.final_level, else_=0)


def _cluster_select():
    """簇 + 代表事件（LATERAL 子查询：已推送取 rep_event_id，否则取等级最高、最早的一条）

    assets 是簇内所有事件资产的并集（去重、排序，没有则为空数组），
    与按资产筛选时「簇内任一事件含该资产」的口径一致。
    """
    rep = (
        select(
            events.c.id, events.c.title, events.c.url, events.c.source,
            events.c.occurred_at, events.c.collected_at,
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
    unnested = func.unnest(events.c.assets).table_valued("asset").render_derived(name="unnested")
    asset = unnested.c.asset
    cluster_assets = (
        select(func.coalesce(func.array_agg(aggregate_order_by(asset.distinct(), asset)), literal_column("'{}'::text[]")))
        .select_from(events.join(unnested, true()))
        .where(events.c.cluster_id == event_clusters.c.id)
        .correlate(event_clusters)
        .scalar_subquery()
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
        cluster_assets.label("assets"),
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
        "url": safe_http_url(row.url),
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
    """首页时间线：按代表事件的发生时间倒序，发生时间相同时按簇 id 倒序（顺序稳定）"""
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
    stmt = stmt.order_by(rep.c.occurred_at.desc(), event_clusters.c.id.desc()).limit(limit)
    async with engine.connect() as conn:
        rows = (await conn.execute(stmt)).all()
    return [_to_dict(row, freshness) for row in rows]


async def cluster_rows(engine: AsyncEngine, cluster_ids: list[int], *, freshness: FreshnessLookup) -> list[dict[str, Any]]:
    """指定簇的时间线行（用于 SSE 增量更新），排序规则与 timeline 一致"""
    if not cluster_ids:
        return []
    stmt, rep = _cluster_select()
    stmt = stmt.where(event_clusters.c.id.in_(cluster_ids)).order_by(rep.c.occurred_at.desc(), event_clusters.c.id.desc())
    async with engine.connect() as conn:
        rows = (await conn.execute(stmt)).all()
    return [_to_dict(row, freshness) for row in rows]


def _local_time(value: datetime | None, *, now: datetime | None = None) -> str:
    """显示上海本地时间：今天省略日期，其他日期保留月日。"""
    if value is None:
        return "—"
    local_value = value.astimezone(LOCAL_TZ)
    local_now = (now or utcnow()).astimezone(LOCAL_TZ)
    fmt = "%H:%M:%S" if local_value.date() == local_now.date() else "%m-%d %H:%M:%S"
    return local_value.strftime(fmt)


async def collector_status(engine: AsyncEngine, *, now: datetime | None = None) -> list[dict[str, Any]]:
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
            "last_success": _local_time(row["last_success_at"], now=now),
            "last_event": _local_time(row["last_event_at"], now=now),
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


def _decimal(value: Any) -> Decimal | None:
    """把画像里的可选数字安全转换为有限 Decimal。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _wallet_dict(row: Any, total_notional: Decimal) -> dict[str, Any]:
    profile = dict(row["profile"] or {})
    labels = [
        value
        for key in ("leaderboard_display_name", "hypurrscan_alias")
        if isinstance((value := profile.get(key)), str) and value
    ]
    tags = profile.get("tags")
    if not isinstance(tags, list):
        tags = []
    note = row["note"]
    account_value = _decimal(profile.get("account_value", profile.get("leaderboard_account_value")))
    pnl_30d = _decimal(profile.get("pnl_30d"))
    win_rate = _decimal(profile.get("win_rate"))
    return {
        "address": row["address"],
        "short_address": f"{row['address'][:6]}…{row['address'][-4:]}",
        "note": note,
        "labels": labels,
        "tags": [str(tag) for tag in tags if str(tag)],
        "display_name": note or next(iter(labels), None),
        "tier": row["tier"],
        "source": row["source"],
        "excluded": bool(row["excluded"]),
        "style": row["style"],
        "profile": profile,
        "profile_updated_at": row["profile_updated_at"],
        "profile_updated": _local_time(row["profile_updated_at"]),
        "account_value": _decimal_text(account_value),
        "account_value_value": account_value,
        "pnl_30d": _decimal_text(pnl_30d),
        "pnl_30d_value": pnl_30d,
        "win_rate": _decimal_text(win_rate),
        "win_rate_value": win_rate,
        "total_notional": _decimal_text(total_notional) or "0",
        "total_notional_value": total_notional,
        "has_position": total_notional > 0,
    }


async def whale_rows(
    engine: AsyncEngine,
    *,
    account_value_min: Decimal | None = None,
    pnl_30d_min: Decimal | None = None,
    win_rate_min: Decimal | None = None,
    style: str | None = None,
    tier: str | None = None,
    has_position: bool | None = None,
    excluded: bool | None = None,
    sort_by: str = "account_value",
    sort_order: str = "desc",
    limit: int = 200,
) -> list[dict[str, Any]]:
    """巨鲸列表：在有限名单上用 Python 处理 JSON 画像筛选，避免坏画像使整页查询失败。"""
    totals = (
        select(
            hl_positions.c.address,
            func.coalesce(func.sum(func.abs(hl_positions.c.notional_usd)), 0).label("total_notional"),
        )
        .where(hl_positions.c.szi != 0)
        .group_by(hl_positions.c.address)
        .subquery()
    )
    stmt = select(hl_wallets, totals.c.total_notional).outerjoin(
        totals, totals.c.address == hl_wallets.c.address
    )
    async with engine.connect() as conn:
        raw_rows = (await conn.execute(stmt)).mappings().all()
    rows = [_wallet_dict(row, Decimal(row["total_notional"] or 0)) for row in raw_rows]
    if account_value_min is not None:
        rows = [row for row in rows if row["account_value_value"] is not None and row["account_value_value"] >= account_value_min]
    if pnl_30d_min is not None:
        rows = [row for row in rows if row["pnl_30d_value"] is not None and row["pnl_30d_value"] >= pnl_30d_min]
    if win_rate_min is not None:
        rows = [row for row in rows if row["win_rate_value"] is not None and row["win_rate_value"] >= win_rate_min]
    if style is not None:
        rows = [row for row in rows if row["style"] == style]
    if tier is not None:
        rows = [row for row in rows if row["tier"] == tier]
    if has_position is not None:
        rows = [row for row in rows if row["has_position"] is has_position]
    if excluded is not None:
        rows = [row for row in rows if row["excluded"] is excluded]

    sort_fields = {
        "account_value": "account_value_value",
        "pnl_30d": "pnl_30d_value",
        "win_rate": "win_rate_value",
        "style": "style",
        "tier": "tier",
        "has_position": "has_position",
        "total_notional": "total_notional_value",
    }
    field = sort_fields.get(sort_by, "account_value_value")
    present = [row for row in rows if row[field] is not None]
    missing = [row for row in rows if row[field] is None]
    reverse = sort_order != "asc"
    present.sort(key=lambda row: (row[field], row["address"]), reverse=reverse)
    missing.sort(key=lambda row: row["address"])
    return (present + missing)[: max(1, min(limit, 200))]


def market_tickers(
    symbols: list[str],
    mids: dict[str, Decimal],
    *,
    enabled: bool,
    connected: bool,
) -> list[dict[str, str | None]]:
    """按配置顺序生成行情条，不把缺价或断线旧价伪装成实时行情。"""
    rows: list[dict[str, str | None]] = []
    for symbol in symbols:
        price = _decimal(mids.get(symbol)) if enabled else None
        status = "未启用" if not enabled else "缺价" if price is None else "实时" if connected else "断线"
        rows.append({"symbol": symbol, "price": _decimal_text(price), "status": status})
    return rows


async def whale_net_positions(
    engine: AsyncEngine,
    *,
    mids: dict[str, Decimal],
    now: datetime,
    stale_after_s: float,
) -> list[dict[str, str]]:
    """按当前 allMids 聚合核心与重点钱包的多、空、净仓位。"""
    cutoff = now - timedelta(seconds=stale_after_s)
    stmt = (
        select(hl_positions)
        .join(hl_wallets, hl_wallets.c.address == hl_positions.c.address)
        .where(
            hl_wallets.c.tier.in_(("focus", "core")),
            hl_wallets.c.excluded.is_(False),
            hl_positions.c.updated_at >= cutoff,
            hl_positions.c.szi != 0,
        )
    )
    async with engine.connect() as conn:
        positions = (await conn.execute(stmt)).mappings().all()
    grouped: dict[str, dict[str, Any]] = {}
    for row in positions:
        mark = _decimal(mids.get(row["coin"]))
        size = _decimal(row["szi"])
        if mark is None or mark <= 0 or size is None or size == 0:
            continue
        values = grouped.setdefault(
            row["coin"],
            {"long": Decimal(0), "short": Decimal(0), "updated_at": row["updated_at"]},
        )
        notional = abs(size) * mark
        values["long" if size > 0 else "short"] += notional
        values["updated_at"] = max(values["updated_at"], row["updated_at"])
    result = []
    for coin, values in sorted(grouped.items()):
        net = values["long"] - values["short"]
        result.append({
            "coin": coin,
            "long_usd": _decimal_text(values["long"]) or "0",
            "short_usd": _decimal_text(values["short"]) or "0",
            "net_usd": _decimal_text(net) or "0",
            "direction": "long" if net > 0 else "short" if net < 0 else "flat",
            "updated_at": values["updated_at"].isoformat(),
        })
    return result


async def whale_detail(
    engine: AsyncEngine,
    address: str,
    *,
    mids: dict[str, Decimal],
    operation_limit: int = 50,
) -> dict[str, Any] | None:
    """读取钱包画像、当前仓位以及 Hyperliquid 事件与成交。"""
    normalized = normalize_address(address)
    async with engine.connect() as conn:
        wallet_row = (
            await conn.execute(select(hl_wallets).where(hl_wallets.c.address == normalized))
        ).mappings().first()
        if wallet_row is None:
            return None
        position_rows = (
            await conn.execute(
                select(hl_positions)
                .where(hl_positions.c.address == normalized, hl_positions.c.szi != 0)
                .order_by(hl_positions.c.coin, hl_positions.c.dex)
            )
        ).mappings().all()
        event_rows = (
            await conn.execute(
                select(events)
                .where(events.c.category == "hyperliquid", events.c.raw["address"].astext == normalized)
                .order_by(events.c.occurred_at.desc(), events.c.id.desc())
                .limit(operation_limit)
            )
        ).mappings().all()
        fill_rows = (
            await conn.execute(
                select(hl_fills)
                .where(hl_fills.c.address == normalized)
                .order_by(hl_fills.c.occurred_at.desc(), hl_fills.c.tid.desc())
                .limit(operation_limit)
            )
        ).mappings().all()

    total_notional = sum((abs(Decimal(row["notional_usd"] or 0)) for row in position_rows), Decimal(0))
    wallet = _wallet_dict(wallet_row, total_notional)
    profile = wallet["profile"]
    positions = []
    for row in position_rows:
        size = Decimal(row["szi"])
        mark = _decimal(mids.get(row["coin"]))
        liquidation = _decimal(row["liquidation_px"])
        distance = abs(mark - liquidation) / mark if mark and mark > 0 and liquidation is not None else None
        notional = abs(size) * mark if mark is not None else _decimal(row["notional_usd"])
        positions.append({
            "dex": row["dex"] or "主 dex",
            "coin": row["coin"],
            "direction": "long" if size > 0 else "short",
            "quantity": _decimal_text(abs(size)),
            "notional_usd": _decimal_text(notional),
            "entry_px": _decimal_text(_decimal(row["entry_px"])),
            "liquidation_px": _decimal_text(liquidation),
            "liquidation_distance": _decimal_text(distance),
            "liquidation_progress": float(max(Decimal(0), min(Decimal(100), (Decimal(1) - (distance or Decimal(1))) * 100))),
            "leverage": _decimal_text(_decimal(row["leverage"])),
            "unrealized_pnl": _decimal_text(_decimal(row["unrealized_pnl"])),
            "updated_at": row["updated_at"].isoformat(),
        })
    operations = [
        {
            "kind": "event",
            "occurred_at": row["occurred_at"],
            "title": row["title"],
            "coin": (row["raw"] or {}).get("coin"),
            "amount_usd": _decimal_text(_decimal(row["amount_usd"])),
        }
        for row in event_rows
    ] + [
        {
            "kind": "fill",
            "occurred_at": row["occurred_at"],
            "title": row["dir"],
            "coin": row["coin"],
            "amount_usd": _decimal_text(Decimal(row["px"]) * Decimal(row["sz"])),
        }
        for row in fill_rows
    ]
    operations.sort(key=lambda item: item["occurred_at"], reverse=True)
    return {
        "wallet": wallet,
        "profile": profile,
        "coverage": dict(profile.get("coverage") or {}),
        "pnl_history": list(profile.get("pnl_history") or []),
        "positions": positions,
        "operations": operations[:operation_limit],
    }
