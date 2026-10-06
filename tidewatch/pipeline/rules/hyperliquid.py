"""Hyperliquid 巨鲸事件分级规则。"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from tidewatch.config import HyperliquidEventsCfg
from tidewatch.hyperliquid.tiers import NON_PROMOTABLE_STYLES
from tidewatch.models import Event, RuleResult
from tidewatch.pipeline.rules.engine import RuleContext


_POSITION_EVENTS = frozenset({"hl_open", "hl_increase", "hl_decrease", "hl_close"})


class HyperliquidRules:
    """按相关名义价值、强平距离和钱包风格对巨鲸事件分级。"""

    def __init__(self, cfg: HyperliquidEventsCfg) -> None:
        self._cfg = cfg

    async def evaluate(self, event: Event, ctx: RuleContext) -> RuleResult:
        style = str(event.raw.get("style") or "").strip().lower()
        if style in NON_PROMOTABLE_STYLES:
            return RuleResult("C", ["hyperliquid.style_suppressed"])

        amount = _decimal(event.amount_usd)
        if event.event_type in _POSITION_EVENTS:
            level = self._notional_level(amount)
        elif event.event_type == "hl_liq_risk":
            level = self._liquidation_risk_level(amount, event.raw.get("liquidation_distance"))
        elif event.event_type == "hl_liquidated":
            threshold = Decimal(str(self._cfg.liquidated_a_level_notional_usd))
            level = "A" if amount >= threshold else "B"
        elif event.event_type == "hl_new_whale":
            level = "B"
        else:
            return RuleResult("C", ["hyperliquid.unknown_event"])
        return RuleResult(level, [f"hyperliquid.{event.event_type}.{level.lower()}"])

    def _notional_level(self, amount: Decimal) -> str:
        if amount >= Decimal(str(self._cfg.s_level_notional_usd)):
            return "S"
        if amount >= Decimal(str(self._cfg.a_level_notional_usd)):
            return "A"
        return "B"

    def _liquidation_risk_level(self, amount: Decimal, value: object) -> str:
        distance = _optional_decimal(value)
        if distance is None or distance > Decimal(str(self._cfg.liquidation_risk_ratio)):
            return "C"
        if amount < Decimal(str(self._cfg.hot_position_notional_usd)):
            return "B"
        if distance <= Decimal(str(self._cfg.liquidation_critical_ratio)):
            return "S"
        return "A"


def _decimal(value: object) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Hyperliquid 事件金额无效") from exc
    if not parsed.is_finite():
        raise ValueError("Hyperliquid 事件金额无效")
    return parsed


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return _decimal(value)
