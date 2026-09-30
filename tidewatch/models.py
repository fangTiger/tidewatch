"""领域模型与等级工具"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

# 等级从低到高
LEVELS = ("C", "B", "A", "S")
_RANK = {level: index for index, level in enumerate(LEVELS)}


def level_rank(level: str | None) -> int:
    """等级序号：C=0 … S=3；None 为 -1"""
    return -1 if level is None else _RANK[level]


def level_ge(a: str | None, b: str) -> bool:
    """a 是否不低于 b"""
    return level_rank(a) >= level_rank(b)


def max_level(*levels: str | None) -> str | None:
    """取最高等级，全部为 None 时返回 None"""
    present = [level for level in levels if level is not None]
    return max(present, key=level_rank) if present else None


@dataclass
class Event:
    """统一事件：采集器负责基础字段，EventSink 负责分级、聚簇等字段"""

    source: str
    category: str
    event_type: str
    occurred_at: datetime
    title: str
    dedupe_key: str
    body: str = ""
    url: str | None = None
    assets: list[str] = field(default_factory=list)
    amount_usd: Decimal | float | None = None  # 数据库 Numeric 读回为 Decimal
    ref_date: date | None = None
    raw: dict[str, Any] = field(default_factory=dict)
    # 以下字段由 EventSink 填充
    id: int | None = None
    collected_at: datetime | None = None
    rule_level: str | None = None
    rule_hits: list[str] = field(default_factory=list)
    final_level: str | None = None
    ai_status: str | None = None
    cluster_id: int | None = None

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValueError(f"事件时间必须带时区：{self.dedupe_key}")


@dataclass
class FetchResult:
    """采集器一次拉取的结果"""

    events: list[Event]
    cursor: dict[str, Any] | None
    records: list[Any] = field(default_factory=list)  # 辅助记录（三期起使用）


@dataclass
class RuleResult:
    """规则分级结果"""

    level: str
    hits: list[str] = field(default_factory=list)
