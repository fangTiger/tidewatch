from datetime import datetime, timezone

import pytest

from tidewatch.models import Event
from tidewatch.pipeline.rules.engine import RuleContext, build_rule_engine
from tidewatch.pipeline.rules.exchange import ExchangeRules

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
CTX = RuleContext(conn=None, now=NOW)


def _ann(exchange: str, kind: str, assets: list[str]) -> Event:
    return Event(
        source=f"{exchange}_ann",
        category="exchange",
        event_type=kind,
        occurred_at=NOW,
        title=f"{exchange} {kind}",
        dedupe_key=f"{exchange}:{kind}:{assets}",
        assets=assets,
        raw={"exchange": exchange},
    )


@pytest.mark.parametrize(
    ("exchange", "kind", "assets", "level"),
    [
        ("binance", "spot_listing", ["XYZ"], "S"),
        ("okx", "spot_listing", ["XYZ"], "A"),
        ("binance", "perp_listing", ["XYZ"], "A"),
        ("okx", "perp_listing", ["XYZ"], "B"),
        ("okx", "delisting", ["XRP"], "A"),
        ("binance", "delisting", ["ABC"], "B"),
        ("binance", "perp_delisting", ["XRP"], "B"),
        ("okx", "deposit_withdraw_suspend", ["ETH"], "A"),
        ("okx", "deposit_withdraw_suspend", ["LSK"], "C"),
        ("binance", "other", ["HYPE"], "C"),
    ],
)
async def test_exchange_levels(exchange, kind, assets, level):
    rules = ExchangeRules(watch_symbols={"BTC", "ETH", "XRP", "HYPE"})
    result = await rules.evaluate(_ann(exchange, kind, assets), CTX)
    assert result.level == level
    assert f"exchange.{exchange}.{kind}" in result.hits


@pytest.mark.parametrize("raw", [{}, {"exchange": "  "}])
async def test_exchange_name_falls_back_to_source(raw):
    event = _ann("binance", "spot_listing", ["XYZ"])
    event.raw = raw  # 采集器漏写（或写成空白）raw.exchange 时按来源名推断
    result = await ExchangeRules(watch_symbols=set()).evaluate(event, CTX)
    assert result.level == "S"


@pytest.mark.parametrize("raw_exchange", ["OKX", " OKX ", " okx"])
async def test_exchange_name_is_normalized(raw_exchange):
    event = _ann("okx", "spot_listing", ["XYZ"])
    event.raw = {"exchange": raw_exchange}
    result = await ExchangeRules(watch_symbols=set()).evaluate(event, CTX)
    assert result.level == "A"
    assert "exchange.okx.spot_listing" in result.hits


async def test_build_rule_engine(settings):
    engine = build_rule_engine(settings)
    assert (await engine.evaluate(_ann("binance", "spot_listing", ["XYZ"]), CTX)).level == "S"
    news = Event(source="jin10", category="news", event_type="flash", occurred_at=NOW,
                 title="美国9月非农就业人口增加8.2万人", dedupe_key="jin10:x")
    assert (await engine.evaluate(news, CTX)).level == "B"
