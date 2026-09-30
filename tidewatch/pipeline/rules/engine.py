"""规则引擎：按事件类别分派到对应规则集"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.models import Event, RuleResult

if TYPE_CHECKING:
    from tidewatch.config import Settings


class RuleContext:
    """规则可用的只读上下文：当前事务连接与处理时刻

    一期规则不需要历史数据；后续期次（ETF 连续流入、内部人买入统计等）在此类上增加只读查询方法。
    规则只能通过 conn 读取数据，不得写库（写入统一由 EventSink 负责）。
    """

    def __init__(self, conn: AsyncConnection | None, now: datetime) -> None:
        self.conn = conn
        self.now = now


class CategoryRules(Protocol):
    """某一类别的规则集"""

    async def evaluate(self, event: Event, ctx: RuleContext) -> RuleResult: ...


class RuleEngine:
    """按 event.category 找到规则集并分级；没有规则集的类别一律为 C"""

    def __init__(self, rules_by_category: dict[str, CategoryRules]) -> None:
        self._rules = rules_by_category

    async def evaluate(self, event: Event, ctx: RuleContext) -> RuleResult:
        rules = self._rules.get(event.category)
        if rules is None:
            return RuleResult("C", ["no_rules"])
        return await rules.evaluate(event, ctx)
