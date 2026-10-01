"""使用当前配置重新计算既有事件的规则等级，全程只读。"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import format_local, utcnow
from tidewatch.config import ConfigError, Settings, load_settings
from tidewatch.models import Event, LEVELS
from tidewatch.pipeline.rules.engine import RuleContext, build_rule_engine
from tidewatch.store.db import create_db_engine
from tidewatch.store.tables import events

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class RegradeChange:
    """单条发生等级变化的事件。"""

    occurred_at: datetime
    old_level: str | None
    new_level: str
    hits: tuple[str, ...]
    title: str


@dataclass(frozen=True)
class RegradeReport:
    """一次只读重新分级的统计结果。"""

    evaluated: int
    old_counts: dict[str, int]
    new_counts: dict[str, int]
    changes: tuple[RegradeChange, ...]


def _empty_counts() -> dict[str, int]:
    return {**{level: 0 for level in reversed(LEVELS)}, "未分级": 0}


def _count(counts: dict[str, int], level: str | None) -> None:
    counts[level if level in LEVELS else "未分级"] += 1


def _to_event(row) -> Event:
    """把数据库行恢复为规则引擎所需的领域事件。"""
    return Event(
        id=row.id,
        source=row.source,
        category=row.category,
        event_type=row.event_type,
        occurred_at=row.occurred_at,
        ref_date=row.ref_date,
        collected_at=row.collected_at,
        title=row.title,
        body=row.body,
        url=row.url,
        assets=list(row.assets),
        amount_usd=row.amount_usd,
        dedupe_key=row.dedupe_key,
        raw=dict(row.raw),
        rule_level=row.rule_level,
        rule_hits=list(row.rule_hits),
        final_level=row.final_level,
        ai_status=row.ai_status,
        cluster_id=row.cluster_id,
    )


async def regrade_events(
    db_engine: AsyncEngine,
    settings: Settings,
    *,
    source: str = "jin10",
    limit: int | None = None,
) -> RegradeReport:
    """读取指定来源的最新事件并用当前规则重新分级，不回写任何字段。"""
    if limit is not None and limit <= 0:
        raise ValueError("limit 必须大于 0")
    stmt = (
        select(events)
        .where(events.c.source == source)
        .order_by(events.c.occurred_at.desc(), events.c.id.desc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)

    rules = build_rule_engine(settings)
    old_counts = _empty_counts()
    new_counts = _empty_counts()
    changes: list[RegradeChange] = []
    async with db_engine.connect() as conn:
        rows = (await conn.execute(stmt)).all()
        ctx = RuleContext(conn, utcnow())
        for row in rows:
            event = _to_event(row)
            result = await rules.evaluate(event, ctx)
            _count(old_counts, event.rule_level)
            _count(new_counts, result.level)
            if event.rule_level != result.level:
                changes.append(
                    RegradeChange(
                        occurred_at=event.occurred_at,
                        old_level=event.rule_level,
                        new_level=result.level,
                        hits=tuple(result.hits),
                        title=event.title,
                    )
                )
    return RegradeReport(len(rows), old_counts, new_counts, tuple(changes))


def format_report(report: RegradeReport, *, include_changes: bool) -> str:
    """把结构化结果格式化为终端表格。"""
    lines = ["等级 | 旧 | 新", "--- | ---: | ---:"]
    for level in (*reversed(LEVELS), "未分级"):
        lines.append(f"{level} | {report.old_counts[level]} | {report.new_counts[level]}")
    lines.append(f"合计 | {report.evaluated} | {report.evaluated}")
    if not include_changes:
        return "\n".join(lines)

    lines.extend(["", "变化事件", "时间 | 旧→新 | 命中规则 | 标题", "--- | --- | --- | ---"])
    if not report.changes:
        lines.append("（无）")
    for change in report.changes:
        old_level = change.old_level or "未分级"
        hits = ",".join(change.hits) or "-"
        lines.append(
            f"{format_local(change.occurred_at)} | {old_level}→{change.new_level} | "
            f"{hits} | {change.title[:60]}"
        )
    return "\n".join(lines)


def _positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("必须大于 0")
    return number


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """解析只读重新分级命令行参数。"""
    parser = argparse.ArgumentParser(description="用当前规则只读重新分级既有事件")
    parser.add_argument("--source", default="jin10", help="事件来源（默认：jin10）")
    parser.add_argument("--limit", type=_positive_int, help="只检查最新 N 条事件")
    parser.add_argument("--changes", action="store_true", help="显示等级发生变化的事件明细")
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace, settings: Settings) -> RegradeReport:
    db_engine = create_db_engine(settings.secrets.database_url)
    try:
        return await regrade_events(db_engine, settings, source=args.source, limit=args.limit)
    finally:
        await db_engine.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    """运行只读重新分级工具。"""
    args = parse_args(argv)
    try:
        settings = load_settings(REPO_ROOT)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2
    report = asyncio.run(_run(args, settings))
    print(format_report(report, include_changes=args.changes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
