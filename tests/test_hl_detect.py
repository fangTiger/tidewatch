from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import tidewatch.hyperliquid.detect as hl_detect
from tidewatch.hyperliquid.detect import (
    PositionSnapshot,
    detect_events,
    detect_events_with_state,
    load_liquidation_risk_state,
    save_liquidation_risk_state,
)


NOW = datetime(2026, 10, 6, 2, 0, tzinfo=timezone.utc)
ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
OTHER = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def _position(
    coin: str,
    szi: str,
    notional: str,
    *,
    at: datetime = NOW,
    dex: str = "",
    entry_px: str | None = "100",
    liquidation_px: str | None = None,
    leverage: str | None = "5",
    unrealized_pnl: str | None = "1000",
    account_value: str | None = "2000000",
    account_notional: str | None = None,
) -> PositionSnapshot:
    return PositionSnapshot(
        address=ADDRESS,
        dex=dex,
        coin=coin,
        szi=Decimal(szi),
        notional_usd=Decimal(notional),
        entry_px=Decimal(entry_px) if entry_px is not None else None,
        liquidation_px=Decimal(liquidation_px) if liquidation_px is not None else None,
        leverage=Decimal(leverage) if leverage is not None else None,
        unrealized_pnl=Decimal(unrealized_pnl) if unrealized_pnl is not None else None,
        account_value=Decimal(account_value) if account_value is not None else None,
        account_notional_usd=Decimal(account_notional) if account_notional is not None else None,
        updated_at=at,
    )


def _zero(coin: str, *, at: datetime = NOW, dex: str = "") -> PositionSnapshot:
    return _position(
        coin,
        "0",
        "0",
        at=at,
        dex=dex,
        entry_px=None,
        liquidation_px=None,
        leverage=None,
        unrealized_pnl=None,
    )


@pytest.mark.parametrize(
    ("coin", "dex", "below", "at_threshold"),
    [
        ("BTC", "", "999999", "1000000"),
        ("SOL", "", "499999", "500000"),
        ("xyz:SILVER", "xyz", "249999", "250000"),
        ("PURR", "", "249999", "250000"),
    ],
)
def test_open_uses_configured_coin_tiers(settings, coin, dex, below, at_threshold):
    assert detect_events(_zero(coin, dex=dex), _position(coin, "10", below, dex=dex), cfg=settings.hyperliquid) == []

    (event,) = detect_events(
        _zero(coin, dex=dex),
        _position(coin, "10", at_threshold, dex=dex),
        cfg=settings.hyperliquid,
    )
    assert event.event_type == "hl_open"
    assert event.amount_usd == Decimal(at_threshold)
    assert event.assets == [coin]
    assert event.raw["direction"] == "long"


def test_increase_and_decrease_require_ratio_and_half_notional_threshold(settings):
    previous = _position("SOL", "100", "2000000")
    ratio_too_small = _position("SOL", "119.99", "2380000")
    amount_too_small = _position("SOL", "125", "2249999")
    increase = _position("SOL", "125", "2250000")
    decrease = _position("SOL", "75", "1750000")

    assert detect_events(previous, ratio_too_small, cfg=settings.hyperliquid, mark_px=Decimal("20000")) == []
    assert detect_events(previous, amount_too_small, cfg=settings.hyperliquid, mark_px=Decimal("9999.96")) == []
    assert [e.event_type for e in detect_events(previous, increase, cfg=settings.hyperliquid, mark_px=Decimal("10000"))] == [
        "hl_increase"
    ]

    fill = {"coin": "SOL", "time": 1791252001000, "tid": 77, "closedPnl": "12345.67", "px": "10000"}
    (event,) = detect_events(previous, decrease, cfg=settings.hyperliquid, mark_px=Decimal("10000"), fill=fill)
    assert event.event_type == "hl_decrease"
    assert event.raw["closed_pnl"] == "12345.67"
    assert event.occurred_at == datetime.fromtimestamp(fill["time"] / 1000, tz=timezone.utc)
    assert event.dedupe_key.endswith(":hl_decrease:77")


def test_close_and_reverse_use_original_and_new_position_notionals(settings):
    previous = _position("ETH", "80", "8000000")
    closed = detect_events(previous, _zero("ETH", at=NOW + timedelta(seconds=5)), cfg=settings.hyperliquid)
    assert [(e.event_type, e.amount_usd, e.raw["direction"]) for e in closed] == [
        ("hl_close", Decimal("8000000"), "long")
    ]

    current = _position("ETH", "-60", "6000000", at=NOW + timedelta(seconds=10))
    reversed_events = detect_events(previous, current, cfg=settings.hyperliquid)
    assert [(e.event_type, e.amount_usd, e.raw["direction"]) for e in reversed_events] == [
        ("hl_close", Decimal("8000000"), "long"),
        ("hl_open", Decimal("6000000"), "short"),
    ]
    assert reversed_events[0].dedupe_key != reversed_events[1].dedupe_key


@pytest.mark.parametrize(
    ("coin", "notional"),
    [
        ("CHIP", "16"),
        ("CRV", "25"),
        ("PAXG", "32"),
        ("RENDER", "62"),
        ("SAGA", "116"),
        ("WLD", "116"),
    ],
)
def test_real_small_closes_below_tier_threshold_are_suppressed(
    settings, coin, notional
):
    previous = _position(coin, "1", notional)

    assert detect_events(previous, _zero(coin), cfg=settings.hyperliquid) == []


@pytest.mark.parametrize(
    ("coin", "old_notional", "new_notional", "expected_types"),
    [
        ("WLD", "250000", "304", ["hl_close"]),
        ("UMA", "1397", "2083", []),
        ("UMA", "1397", "250000", ["hl_open"]),
    ],
)
def test_reverse_applies_tier_threshold_to_each_side_independently(
    settings, coin, old_notional, new_notional, expected_types
):
    previous = _position(coin, "1", old_notional)
    current = _position(
        coin,
        "-1",
        new_notional,
        at=NOW + timedelta(seconds=1),
    )

    assert [
        event.event_type
        for event in detect_events(previous, current, cfg=settings.hyperliquid)
    ] == expected_types


def test_snapshot_dedupe_key_and_occurred_at_are_stable_across_retries(settings):
    current = _position("BTC", "10", "1000000", at=NOW + timedelta(milliseconds=123))
    first = detect_events(_zero("BTC"), current, cfg=settings.hyperliquid)
    second = detect_events(_zero("BTC"), current, cfg=settings.hyperliquid)
    assert first[0].occurred_at == current.updated_at
    assert first[0].dedupe_key == second[0].dedupe_key
    assert first[0].dedupe_key == f"hl:{ADDRESS}:BTC:hl_open:1791252000123"


def test_liquidation_risk_hysteresis_and_a_to_s_transition(settings):
    large_a = _position("BTC", "120", "12000000", liquidation_px="95")
    entered_a = detect_events_with_state(
        large_a,
        large_a,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
        liquidation_risk_state=None,
    )
    assert [(e.event_type, e.raw["liquidation_risk_level"]) for e in entered_a.events] == [("hl_liq_risk", "A")]
    assert entered_a.liquidation_risk_state == "A"

    jitter = _position("BTC", "120", "12000000", liquidation_px="94.8", at=NOW + timedelta(minutes=1))
    suppressed = detect_events_with_state(
        large_a,
        jitter,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
        liquidation_risk_state="A",
    )
    assert suppressed.events == [] and suppressed.liquidation_risk_state == "A"

    critical = _position("BTC", "120", "12000000", liquidation_px="98", at=NOW + timedelta(minutes=2))
    entered_s = detect_events_with_state(
        jitter,
        critical,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
        liquidation_risk_state="A",
    )
    assert [(e.event_type, e.raw["liquidation_risk_level"]) for e in entered_s.events] == [("hl_liq_risk", "S")]
    assert entered_s.liquidation_risk_state == "S"

    recovered = _position("BTC", "120", "12000000", liquidation_px="93.9", at=NOW + timedelta(minutes=3))
    cleared = detect_events_with_state(
        critical,
        recovered,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
        liquidation_risk_state="S",
    )
    assert cleared.events == [] and cleared.liquidation_risk_state is None


def test_liquidation_risk_b_upgrades_only_after_large_position_enters_a_threshold(settings):
    small = _position("BTC", "90", "9000000", liquidation_px="95")
    entered_b = detect_events_with_state(
        small,
        small,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
    )
    assert entered_b.liquidation_risk_state == "B"

    large_outside_a = _position(
        "BTC",
        "120",
        "12000000",
        liquidation_px="94.5",
        at=NOW + timedelta(minutes=1),
    )
    held_b = detect_events_with_state(
        small,
        large_outside_a,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
        liquidation_risk_state="B",
    )
    assert held_b.events == [] and held_b.liquidation_risk_state == "B"

    large_inside_a = _position(
        "BTC",
        "120",
        "12000000",
        liquidation_px="95",
        at=NOW + timedelta(minutes=2),
    )
    upgraded = detect_events_with_state(
        large_outside_a,
        large_inside_a,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
        liquidation_risk_state=held_b.liquidation_risk_state,
    )
    assert [(event.event_type, event.raw["liquidation_risk_level"]) for event in upgraded.events] == [
        ("hl_liq_risk", "A")
    ]
    assert upgraded.liquidation_risk_state == "A"


def test_reverse_fill_closed_pnl_belongs_only_to_close_event(settings):
    previous = _position("ETH", "80", "8000000")
    current = _position("ETH", "-60", "6000000", at=NOW + timedelta(seconds=1))
    fill = {
        "coin": "ETH",
        "time": 1791252001000,
        "tid": 99,
        "px": "100000",
        "sz": "140",
        "closedPnl": "-250000",
    }
    closed, opened = detect_events(previous, current, cfg=settings.hyperliquid, fill=fill)
    assert closed.event_type == "hl_close" and closed.raw["closed_pnl"] == "-250000"
    assert opened.event_type == "hl_open" and opened.raw["closed_pnl"] is None
    assert opened.raw["unrealized_pnl"] == "1000"


def test_small_position_liquidation_risk_is_b_and_high_leverage_is_attribute(settings):
    position = _position("SOL", "100", "2000000", liquidation_px="95", leverage="20", account_value="100000")
    result = detect_events_with_state(
        position,
        position,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
        liquidation_risk_state=None,
    )
    (event,) = result.events
    assert event.raw["liquidation_risk_level"] == "B"
    assert event.raw["high_leverage"] is True
    assert event.raw["configured_leverage"] == "20"
    assert event.raw["actual_leverage"] == "20"


def test_actual_leverage_uses_wallet_total_notional_when_available(settings):
    position = _position(
        "SOL",
        "100",
        "2000000",
        liquidation_px="95",
        leverage="5",
        account_value="1000000",
        account_notional="11000000",
    )
    (event,) = detect_events_with_state(
        position,
        position,
        cfg=settings.hyperliquid,
        mark_px=Decimal("100"),
    ).events
    assert event.raw["actual_leverage"] == "11"
    assert event.raw["high_leverage"] is True


def test_liquidated_requires_matching_normalized_address(settings):
    previous = _position("BTC", "60", "6000000")
    current = _zero("BTC", at=NOW + timedelta(seconds=1))
    own_fill = {
        "coin": "BTC",
        "time": 1791252001000,
        "tid": 88,
        "px": "100000",
        "sz": "60",
        "closedPnl": "-500000",
        "liquidation": {"liquidatedUser": ADDRESS.upper().replace("0X", "0x"), "markPx": "100000", "method": "market"},
    }
    events = detect_events(previous, current, cfg=settings.hyperliquid, fill=own_fill)
    assert [event.event_type for event in events] == ["hl_close", "hl_liquidated"]
    liquidated = events[-1]
    assert liquidated.amount_usd == Decimal("6000000")
    assert liquidated.raw["liquidation"]["method"] == "market"

    counterparty = dict(own_fill, tid=89, liquidation={**own_fill["liquidation"], "liquidatedUser": OTHER})
    assert [event.event_type for event in detect_events(previous, current, cfg=settings.hyperliquid, fill=counterparty)] == [
        "hl_close"
    ]


def test_liquidated_event_is_derived_from_fill_without_position_snapshot(settings):
    fill = {
        "coin": "BTC",
        "time": 1791252001000,
        "tid": 90,
        "px": "100000",
        "sz": "10",
        "startPosition": "-60",
        "dir": "Liquidated Short",
        "closedPnl": "-500000",
        "liquidation": {"liquidatedUser": ADDRESS},
    }

    event = hl_detect.detect_liquidation_event(ADDRESS, fill, cfg=settings.hyperliquid)

    assert event is not None
    assert event.amount_usd == Decimal("6000000")
    assert event.raw["direction"] == "short"
    assert event.raw["quantity"] == "10"
    assert event.dedupe_key == f"hl:{ADDRESS}:BTC:hl_liquidated:90"


async def test_liquidation_risk_state_persists_in_hl_state(engine):
    async with engine.begin() as conn:
        assert await load_liquidation_risk_state(conn, ADDRESS, "", "BTC") is None
        await save_liquidation_risk_state(conn, ADDRESS, "", "BTC", "A", NOW)
    async with engine.connect() as conn:
        assert await load_liquidation_risk_state(conn, ADDRESS.upper().replace("0X", "0x"), "", "BTC") == "A"
    async with engine.begin() as conn:
        await save_liquidation_risk_state(conn, ADDRESS, "", "BTC", None, NOW + timedelta(minutes=1))
        assert await load_liquidation_risk_state(conn, ADDRESS, "", "BTC") is None


def test_recorded_hyperliquid_fixtures_are_offline_small_and_cover_real_shapes():
    names = {
        "user_fills.json",
        "user_fills_by_time.json",
        "user_twap_slice_fills.json",
        "all_mids_sample.json",
        "spot_clearinghouse_state.json",
        "user_abstraction.json",
    }
    for name in names:
        path = FIXTURES / name
        assert path.stat().st_size < 100_000
        json.loads(path.read_text(encoding="utf-8"))

    fills = json.loads((FIXTURES / "user_fills.json").read_text(encoding="utf-8"))
    assert any(fill.get("liquidation", {}).get("liquidatedUser") for fill in fills)
    twap = json.loads((FIXTURES / "user_twap_slice_fills.json").read_text(encoding="utf-8"))
    assert twap[0]["fill"]["hash"] == "0x" + "0" * 64 and twap[0]["twapId"]
    assert set(json.loads((FIXTURES / "all_mids_sample.json").read_text(encoding="utf-8"))) == {
        "BTC",
        "ETH",
        "SOL",
        "HYPE",
    }
