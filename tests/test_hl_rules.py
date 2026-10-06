from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tidewatch.models import Event
from tidewatch.pipeline.rules.engine import RuleContext, build_rule_engine
from tidewatch.pipeline.rules.hyperliquid import HyperliquidRules


NOW = datetime(2026, 10, 6, 2, 0, tzinfo=timezone.utc)


def _event(
    event_type: str,
    amount: str | None,
    *,
    raw: dict | None = None,
) -> Event:
    return Event(
        source="hl_monitor",
        category="hyperliquid",
        event_type=event_type,
        occurred_at=NOW,
        title=event_type,
        dedupe_key=f"hl:test:{event_type}:{amount}:{raw}",
        assets=["BTC"],
        amount_usd=Decimal(amount) if amount is not None else None,
        raw=raw or {},
    )


@pytest.mark.parametrize(
    ("amount", "level"),
    [("4999999.99", "B"), ("5000000", "A"), ("49999999.99", "A"), ("50000000", "S")],
)
async def test_position_event_amount_boundaries(settings, amount, level):
    result = await HyperliquidRules(settings.hyperliquid.events).evaluate(
        _event("hl_open", amount), RuleContext(None, NOW)
    )
    assert result.level == level
    assert f"hyperliquid.hl_open.{level.lower()}" in result.hits


@pytest.mark.parametrize("event_type", ["hl_open", "hl_increase", "hl_decrease", "hl_close"])
async def test_all_position_event_types_share_notional_grading(settings, event_type):
    result = await HyperliquidRules(settings.hyperliquid.events).evaluate(
        _event(event_type, "5000000"), RuleContext(None, NOW)
    )
    assert result.level == "A"


@pytest.mark.parametrize(
    ("amount", "distance", "level"),
    [("12000000", "0.02", "S"), ("12000000", "0.05", "A"), ("9999999.99", "0.05", "B")],
)
async def test_liquidation_risk_levels(settings, amount, distance, level):
    result = await HyperliquidRules(settings.hyperliquid.events).evaluate(
        _event("hl_liq_risk", amount, raw={"liquidation_distance": distance}),
        RuleContext(None, NOW),
    )
    assert result.level == level


@pytest.mark.parametrize("amount,level", [("4999999.99", "B"), ("5000000", "A")])
async def test_liquidated_amount_boundary(settings, amount, level):
    result = await HyperliquidRules(settings.hyperliquid.events).evaluate(
        _event("hl_liquidated", amount), RuleContext(None, NOW)
    )
    assert result.level == level


async def test_new_whale_is_b_and_unknown_event_is_c(settings):
    rules = HyperliquidRules(settings.hyperliquid.events)
    assert (await rules.evaluate(_event("hl_new_whale", "5000000"), RuleContext(None, NOW))).level == "B"
    assert (await rules.evaluate(_event("hl_unknown", "999999999"), RuleContext(None, NOW))).level == "C"


@pytest.mark.parametrize("style", ["market_maker", "high_frequency"])
async def test_market_maker_and_high_frequency_wallets_are_downgraded_to_c(settings, style):
    result = await HyperliquidRules(settings.hyperliquid.events).evaluate(
        _event("hl_open", "60000000", raw={"style": style}), RuleContext(None, NOW)
    )
    assert result.level == "C"
    assert "hyperliquid.style_suppressed" in result.hits


async def test_rule_engine_registers_hyperliquid_without_changing_other_categories(settings):
    engine = build_rule_engine(settings)
    whale = await engine.evaluate(_event("hl_open", "5000000"), RuleContext(None, NOW))
    unknown = await engine.evaluate(
        Event(
            source="demo",
            category="demo",
            event_type="demo",
            occurred_at=NOW,
            title="demo",
            dedupe_key="demo:1",
        ),
        RuleContext(None, NOW),
    )
    assert whale.level == "A"
    assert unknown.level == "C" and unknown.hits == ["no_rules"]
