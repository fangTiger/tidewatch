from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, insert, select
from websockets.asyncio.server import serve

from fakes import FakeHealth, async_timeout
from tidewatch.hyperliquid.discovery import (
    QualificationRequest,
    TradeAggregator,
    TradeDiscovery,
    load_trade_coins,
    parse_trade_message,
)
from tidewatch.hyperliquid.monitor import ReconcilePriority
from tidewatch.hyperliquid.registry import WalletRegistry
from tidewatch.hyperliquid.runtime import HyperliquidRuntime
from tidewatch.hyperliquid.ws import HyperliquidWsManager
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.tables import events, hl_positions, hl_state, hl_wallets


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"
ZERO_HASH = "0x" + "0" * 64


def _trade(
    *,
    address: str,
    time_ms: int,
    px: str,
    sz: str,
    side: str = "B",
    coin: str = "BTC",
    tid: int = 1,
    trade_hash: str = "0xabc",
) -> dict:
    other = "0x9999999999999999999999999999999999999999"
    users = [address, other] if side == "B" else [other, address]
    return {
        "coin": coin,
        "side": side,
        "px": px,
        "sz": sz,
        "hash": trade_hash,
        "time": time_ms,
        "tid": tid,
        "users": users,
    }


def test_real_trades_fixture_parses_taker_side_users_and_hip3():
    message = json.loads((FIXTURES / "trades_ws_sample.json").read_text(encoding="utf-8"))

    trades = parse_trade_message(message)

    assert trades[0].taker == message["data"][0]["users"][1]
    assert trades[1].taker == message["data"][1]["users"][0]
    assert trades[0].notional_usd == Decimal("5374.922800")
    assert trades[-1].coin == "xyz:GOLD"
    assert {trade.side for trade in trades} == {"A", "B"}


def test_three_second_windows_do_not_cross_boundary_and_twap_zero_hash_is_not_merged():
    address = "0x1111111111111111111111111111111111111111"
    aggregator = TradeAggregator(window_seconds=3)
    first = parse_trade_message(
        {
            "channel": "trades",
            "data": [
                _trade(
                    address=address,
                    time_ms=2_999,
                    px="100",
                    sz="25000",
                    tid=1,
                    trade_hash=ZERO_HASH,
                ),
                _trade(
                    address=address,
                    time_ms=2_999,
                    px="100",
                    sz="25000",
                    tid=2,
                    trade_hash=ZERO_HASH,
                ),
            ],
        }
    )
    second = parse_trade_message(
        {
            "channel": "trades",
            "data": [
                _trade(
                    address=address,
                    time_ms=3_000,
                    px="100",
                    sz="50000",
                    tid=3,
                )
            ],
        }
    )

    for trade in [*first, *second]:
        aggregator.add(trade)
    closed = aggregator.pop_due(6_000)

    assert [(window.start_ms, window.end_ms) for window in closed] == [
        (0, 3_000),
        (3_000, 6_000),
    ]
    assert [window.notional_usd for window in closed] == [
        Decimal("5000000"),
        Decimal("5000000"),
    ]
    assert [window.trade_count for window in closed] == [2, 1]


def test_replayed_normal_trade_is_deduplicated_without_dropping_distinct_twap_slices():
    address = "0x1111111111111111111111111111111111111111"
    normal = _trade(address=address, time_ms=1_000, px="100", sz="1", tid=1)
    twap_one = _trade(
        address=address,
        time_ms=1_100,
        px="100",
        sz="1",
        tid=2,
        trade_hash=ZERO_HASH,
    )
    twap_two = _trade(
        address=address,
        time_ms=1_200,
        px="100",
        sz="1",
        tid=3,
        trade_hash=ZERO_HASH,
    )
    aggregator = TradeAggregator(window_seconds=3)

    for payload in (normal, normal, twap_one, twap_two):
        trade = parse_trade_message({"channel": "trades", "data": [payload]})[0]
        aggregator.add(trade)

    [window] = aggregator.pop_due(3_000)
    assert window.trade_count == 3
    assert window.notional_usd == Decimal("300")


@async_timeout()
async def test_many_below_threshold_windows_do_not_persist_discovery_state(
    engine, settings, clock
):
    discovery = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=WalletRegistry(engine, clock=clock),
        monitor=None,
        rest=None,
        sink=None,
        clock=clock,
    )
    trades = [
        _trade(
            address=f"0x{index + 1:040x}",
            time_ms=1_000,
            px="100",
            sz="1",
            tid=index + 1,
            trade_hash=f"0x{index + 1:064x}",
        )
        for index in range(200)
    ]

    await discovery.handle_message({"channel": "trades", "data": trades})
    await discovery.flush_due(3_000)

    async with engine.connect() as conn:
        count = await conn.scalar(
            select(func.count()).select_from(hl_state).where(
                hl_state.c.name.like("discovery_window:%")
            )
        )
    assert count == 0


@async_timeout()
async def test_discovery_summary_reports_interval_counts_and_resets(
    engine, settings, clock, caplog
):
    settings.hyperliquid.frequencies.discovery_summary_interval_s = 300
    focus = "0x1111111111111111111111111111111111111111"
    high_frequency = "0x2222222222222222222222222222222222222222"
    not_qualified = "0x3333333333333333333333333333333333333333"
    promoted = "0x4444444444444444444444444444444444444444"
    below_threshold = "0x5555555555555555555555555555555555555555"

    class Rest:
        async def try_info(self, request_type: str, **params):
            assert request_type == "clearinghouseState"
            account_value = "1000000" if params["user"] == promoted else "999999"
            return True, {
                "marginSummary": {"accountValue": account_value, "totalNtlPos": "0"},
                "assetPositions": [],
            }

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {
                    "address": focus,
                    "tier": "focus",
                    "source": "manual",
                    "style": None,
                },
                {
                    "address": high_frequency,
                    "tier": "candidate",
                    "source": "leaderboard",
                    "style": "high_frequency",
                },
            ],
        )
    discovery = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=WalletRegistry(engine, clock=clock),
        monitor=None,
        rest=Rest(),
        sink=None,
        clock=clock,
    )
    trades = [
        _trade(address=focus, time_ms=1_000, px="100", sz="60000", tid=1),
        _trade(address=high_frequency, time_ms=1_000, px="100", sz="70000", tid=2),
        _trade(address=not_qualified, time_ms=1_000, px="100", sz="80000", tid=3),
        _trade(
            address=promoted,
            time_ms=1_000,
            px="100",
            sz="90000",
            coin="SOL",
            tid=4,
        ),
        _trade(address=below_threshold, time_ms=1_000, px="100", sz="40000", tid=5),
    ]
    caplog.set_level("INFO", logger="tidewatch.hyperliquid.discovery")

    await discovery.handle_message({"channel": "trades", "data": trades})
    await discovery.flush_due(3_000)
    discovery.record_query_limit_deferral()
    clock.advance(seconds=300)
    await discovery.flush_due(3_000)

    summaries = [
        record.message for record in caplog.records if "成交流发现摘要" in record.message
    ]
    assert summaries == [
        "成交流发现摘要：处理成交 5 笔，关闭窗口 5 个，最大窗口名义 "
        "9000000 美元（SOL，0x4444…4444），≥门槛窗口 4 个，资格查询 2 次，"
        "达标晋升 1 个，不达标缓存 1 个，高频跳过 1 个，因查询限额顺延 1 次"
    ]

    clock.advance(seconds=300)
    await discovery.flush_due(3_000)
    summaries = [
        record.message for record in caplog.records if "成交流发现摘要" in record.message
    ]
    assert summaries[-1] == (
        "成交流发现摘要：处理成交 0 笔，关闭窗口 0 个，最大窗口名义 无，"
        "≥门槛窗口 0 个，资格查询 0 次，达标晋升 0 个，不达标缓存 0 个，"
        "高频跳过 0 个，因查询限额顺延 0 次"
    )


@async_timeout()
async def test_trade_counters_are_capped_and_inactive_addresses_are_evicted(
    engine, settings, clock
):
    busy = "0x1111111111111111111111111111111111111111"
    inactive = "0x2222222222222222222222222222222222222222"
    discovery = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=WalletRegistry(engine, clock=clock),
        monitor=None,
        rest=None,
        sink=None,
        clock=clock,
    )
    busy_trades = [
        _trade(
            address=busy,
            time_ms=index,
            px="1",
            sz="1",
            tid=index,
            trade_hash=f"0x{index:064x}",
        )
        for index in range(1_000)
    ]

    await discovery.handle_message(
        {
            "channel": "trades",
            "data": [
                _trade(
                    address=inactive,
                    time_ms=0,
                    px="1",
                    sz="1",
                    tid=2_000,
                ),
                *busy_trades,
            ],
        }
    )

    assert len(discovery._taker_times[busy]) == 301
    await discovery.flush_due(3_603_000)
    assert discovery.aggregator._windows == {}
    assert discovery._taker_times == {}


@async_timeout()
async def test_expired_discovery_state_is_cleaned_without_touching_other_state(
    engine, settings, clock
):
    settings.hyperliquid.frequencies.discovery_state_retention_s = 7_200
    settings.hyperliquid.frequencies.discovery_cleanup_interval_s = 300
    discovery = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=WalletRegistry(engine, clock=clock),
        monitor=None,
        rest=None,
        sink=None,
        clock=clock,
    )
    rows = [
        {
            "name": "discovery_window:old",
            "cursor": {"processed": True},
            "updated_at": clock.now - timedelta(hours=2, seconds=1),
        },
        {
            "name": "discovery_window:fresh",
            "cursor": {"processed": True},
            "updated_at": clock.now - timedelta(hours=1),
        },
        {
            "name": "discovery_cache:old",
            "cursor": {"expires_at": (clock.now - timedelta(seconds=1)).isoformat()},
            "updated_at": clock.now - timedelta(hours=1, seconds=1),
        },
        {
            "name": "discovery_cache:fresh",
            "cursor": {"expires_at": (clock.now + timedelta(seconds=1)).isoformat()},
            "updated_at": clock.now - timedelta(minutes=59),
        },
        {
            "name": "monitor_baseline:old",
            "cursor": {},
            "updated_at": clock.now - timedelta(days=1),
        },
    ]
    async with engine.begin() as conn:
        await conn.execute(insert(hl_state), rows)

    deleted = await discovery.cleanup_expired_state()

    async with engine.connect() as conn:
        names = set((await conn.execute(select(hl_state.c.name))).scalars())
    assert deleted == 2
    assert names == {
        "discovery_window:fresh",
        "discovery_cache:fresh",
        "monitor_baseline:old",
    }


@async_timeout()
async def test_runtime_periodically_cleans_discovery_state(engine, settings, clock):
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        FakeHealth(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    cleanups = 0
    intervals: list[float] = []

    async def cleanup() -> int:
        nonlocal cleanups
        cleanups += 1
        return 0

    async def wait_or_stop(interval: float) -> bool:
        intervals.append(interval)
        return cleanups == 2

    runtime.discovery.cleanup_expired_state = cleanup
    runtime._wait_or_stop = wait_or_stop
    try:
        await asyncio.wait_for(runtime.run_discovery_cleanup(), timeout=1)
    finally:
        await runtime.close_resources()

    assert cleanups == 2
    assert intervals == [
        settings.hyperliquid.frequencies.discovery_cleanup_interval_s,
        settings.hyperliquid.frequencies.discovery_cleanup_interval_s,
    ]


@async_timeout()
async def test_trade_coins_come_from_main_meta_and_allowlisted_official_perp_dexes():
    responses = {
        "perpDexs": json.loads(
            (FIXTURES / "perp_dexs_sample.json").read_text(encoding="utf-8")
        ),
        "main": json.loads(
            (FIXTURES / "meta_main_sample.json").read_text(encoding="utf-8")
        ),
        "xyz": json.loads(
            (FIXTURES / "meta_xyz_sample.json").read_text(encoding="utf-8")
        ),
    }
    calls: list[tuple[str, str | None]] = []

    class Rest:
        async def info(self, request_type: str, *, budget: str, **params):
            calls.append((request_type, params.get("dex")))
            if request_type == "perpDexs":
                return responses["perpDexs"]
            return responses[params.get("dex") or "main"]

    coins = await load_trade_coins(Rest(), ["xyz", "not-official"])

    assert coins == ("BTC", "ETH", "xyz:GOLD", "xyz:XYZ100")
    assert calls == [("perpDexs", None), ("meta", None), ("meta", "xyz")]


@async_timeout()
async def test_candidate_is_unknown_known_wallet_triggers_reconcile_and_high_frequency_is_skipped(
    engine, settings, clock, caplog
):
    candidate = "0x1111111111111111111111111111111111111111"
    focus = "0x2222222222222222222222222222222222222222"
    high_frequency = "0x3333333333333333333333333333333333333333"
    market_maker = "0x4444444444444444444444444444444444444444"
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {
                    "address": candidate,
                    "tier": "candidate",
                    "source": "leaderboard",
                    "style": None,
                },
                {
                    "address": focus,
                    "tier": "focus",
                    "source": "manual",
                    "style": None,
                },
                {
                    "address": market_maker,
                    "tier": "candidate",
                    "source": "leaderboard",
                    "style": "market_maker",
                },
            ],
        )
    scheduled: list[QualificationRequest] = []
    reconciled: list[str] = []
    discovery = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=WalletRegistry(engine, clock=clock),
        monitor=None,
        rest=None,
        sink=None,
        clock=clock,
        schedule_query=scheduled.append,
        trigger_reconcile=reconciled.append,
    )
    trades = [
        _trade(address=candidate, time_ms=1_000, px="100", sz="50000", tid=1),
        _trade(address=focus, time_ms=1_000, px="100", sz="50000", tid=2),
        _trade(address=market_maker, time_ms=1_000, px="100", sz="50000", tid=3),
    ]
    trades.extend(
        _trade(
            address=high_frequency,
            time_ms=1_000 + index,
            px="100",
            sz="200",
            tid=10 + index,
            trade_hash=f"0x{index:064x}",
        )
        for index in range(301)
    )

    await discovery.handle_message({"channel": "trades", "data": trades})
    await discovery.flush_due(3_000)

    assert [(item.address, item.coin, item.notional_usd) for item in scheduled] == [
        (candidate, "BTC", Decimal("5000000"))
    ]
    assert reconciled == [focus]
    assert any("高频" in record.message for record in caplog.records)


@async_timeout()
async def test_failed_qualification_is_cached_for_one_hour(engine, settings, clock):
    address = "0x1111111111111111111111111111111111111111"
    queries = 0

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal queries
            queries += 1
            return True, {
                "marginSummary": {"accountValue": "999999", "totalNtlPos": "4999999"},
                "assetPositions": [],
            }

    scheduled: list[QualificationRequest] = []
    registry = WalletRegistry(engine, clock=clock)
    discovery = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=registry,
        monitor=None,
        rest=Rest(),
        sink=None,
        clock=clock,
        schedule_query=scheduled.append,
    )
    request = QualificationRequest(
        address=address,
        coin="BTC",
        start_ms=0,
        end_ms=3_000,
        notional_usd=Decimal("5000000"),
        trade_count=1,
    )

    assert await discovery.process_qualification(request)
    assert queries == 1
    assert not await discovery.may_schedule(address)
    clock.advance(minutes=59)
    assert not await discovery.may_schedule(address)
    clock.advance(minutes=1)
    assert await discovery.may_schedule(address)


@pytest.mark.parametrize(
    ("position_value", "account_value"),
    [("5000000", "999999"), ("4999999", "1000000")],
)
@async_timeout()
async def test_position_or_account_value_boundary_promotes_and_emits_b_event(
    engine, settings, clock, position_value, account_value
):
    address = "0x1111111111111111111111111111111111111111"

    class Rest:
        async def try_info(self, request_type: str, **params):
            assert request_type == "clearinghouseState"
            return True, {
                "marginSummary": {
                    "accountValue": account_value,
                    "totalNtlPos": position_value,
                },
                "assetPositions": [
                    {
                        "type": "oneWay",
                        "position": {
                            "coin": "BTC",
                            "szi": "50",
                            "positionValue": position_value,
                            "entryPx": "100000",
                            "liquidationPx": "90000",
                            "leverage": {"type": "cross", "value": 10},
                            "unrealizedPnl": "0",
                        },
                    }
                ],
            }

    registry = WalletRegistry(engine, focus_limit=2, clock=clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    discovery = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=registry,
        monitor=None,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    request = QualificationRequest(
        address=address,
        coin="BTC",
        start_ms=int(clock.now.timestamp() * 1000) - 3_000,
        end_ms=int(clock.now.timestamp() * 1000),
        notional_usd=Decimal("5000000"),
        trade_count=2,
    )
    try:
        assert await discovery.process_qualification(request)
    finally:
        await sink.stop()

    async with engine.connect() as conn:
        wallet = (await conn.execute(select(hl_wallets))).mappings().one()
        event = (await conn.execute(select(events))).mappings().one()
        baseline = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"monitor_baseline:{address}"
            )
        )
    assert wallet["tier"] == "focus" and wallet["source"] == "discovered"
    assert event["source"] == "hl_discovery"
    assert event["event_type"] == "hl_new_whale"
    assert event["rule_level"] == "B"
    assert event["dedupe_key"] == f"hl:{address}:BTC:hl_new_whale:{request.end_ms}"
    assert event["occurred_at"] == datetime.fromtimestamp(
        request.end_ms / 1000, tz=timezone.utc
    )
    assert baseline["established_at"] == event["occurred_at"].isoformat()


@async_timeout()
async def test_discovered_promotion_replaces_lowest_unprotected_focus(engine, clock):
    protected = "0x1111111111111111111111111111111111111111"
    automatic = "0x2222222222222222222222222222222222222222"
    discovered = "0x3333333333333333333333333333333333333333"
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {
                    "address": protected,
                    "tier": "focus",
                    "source": "manual",
                    "profile": {"manual_focus": True},
                },
                {
                    "address": automatic,
                    "tier": "focus",
                    "source": "leaderboard",
                    "profile": {},
                },
            ],
        )
        await conn.execute(
            insert(hl_positions),
            [
                {
                    "address": protected,
                    "dex": "",
                    "coin": "BTC",
                    "szi": Decimal("1"),
                    "notional_usd": Decimal("100000"),
                    "updated_at": clock.now,
                },
                {
                    "address": automatic,
                    "dex": "",
                    "coin": "ETH",
                    "szi": Decimal("1"),
                    "notional_usd": Decimal("200000"),
                    "updated_at": clock.now,
                },
            ],
        )
    registry = WalletRegistry(engine, focus_limit=2, clock=clock)

    result = await registry.promote_discovered(
        discovered,
        total_notional=Decimal("5000000"),
        last_trade_at=clock.now,
    )

    assert result.promoted
    assert result.replaced == automatic
    async with engine.connect() as conn:
        tiers = dict((await conn.execute(select(hl_wallets.c.address, hl_wallets.c.tier))).all())
    assert tiers == {
        protected: "focus",
        automatic: "candidate",
        discovered: "focus",
    }


@async_timeout()
async def test_discovered_wallet_stays_candidate_when_all_focus_wallets_are_protected(
    engine, clock
):
    protected = [f"0x{index + 1:040x}" for index in range(2)]
    discovered = "0x3333333333333333333333333333333333333333"
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {
                    "address": address,
                    "tier": "focus",
                    "source": "manual",
                    "profile": {"manual_focus": True},
                }
                for address in protected
            ],
        )
    registry = WalletRegistry(engine, focus_limit=2, clock=clock)

    result = await registry.promote_discovered(
        discovered,
        total_notional=Decimal("5000000"),
        last_trade_at=clock.now,
    )

    assert not result.promoted
    assert result.reason == "no_replaceable_wallet"
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                select(hl_wallets).where(hl_wallets.c.address == discovered)
            )
        ).mappings().one()
    assert row["tier"] == "candidate" and row["source"] == "discovered"


@async_timeout()
async def test_processed_window_persists_and_restart_does_not_emit_again(
    engine, settings, clock
):
    address = "0x1111111111111111111111111111111111111111"

    class Rest:
        async def try_info(self, request_type: str, **params):
            return True, {
                "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0"},
                "assetPositions": [],
            }

    registry = WalletRegistry(engine, clock=clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    first = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=registry,
        monitor=None,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    message = {
        "channel": "trades",
        "data": [
            _trade(address=address, time_ms=1_000, px="100", sz="50000", tid=1)
        ],
    }
    try:
        await first.handle_message(message)
        await first.flush_due(3_000)

        restarted = TradeDiscovery(
            settings.hyperliquid,
            engine=engine,
            registry=registry,
            monitor=None,
            rest=Rest(),
            sink=sink,
            clock=clock,
        )
        await restarted.handle_message(message)
        await restarted.flush_due(3_000)
    finally:
        await sink.stop()

    async with engine.connect() as conn:
        count = await conn.scalar(
            select(func.count()).select_from(hl_state).where(
                hl_state.c.name.like("discovery_window:%")
            )
        )
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name.like("discovery_window:%")
            )
        )
        event_count = await conn.scalar(select(func.count()).select_from(events))
    assert count == 1
    assert cursor["status"] == "qualified" and cursor["processed"] is True
    assert event_count == 1


@async_timeout()
async def test_queued_qualification_is_restored_after_restart(engine, settings, clock):
    address = "0x1111111111111111111111111111111111111111"
    queued: list[QualificationRequest] = []
    first = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=WalletRegistry(engine, clock=clock),
        monitor=None,
        rest=None,
        sink=None,
        clock=clock,
        schedule_query=queued.append,
    )
    message = {
        "channel": "trades",
        "data": [
            _trade(address=address, time_ms=1_000, px="100", sz="50000", tid=1)
        ],
    }
    await first.handle_message(message)
    await first.flush_due(3_000)
    assert len(queued) == 1

    restored: list[QualificationRequest] = []
    restarted = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=WalletRegistry(engine, clock=clock),
        monitor=None,
        rest=None,
        sink=None,
        clock=clock,
        schedule_query=restored.append,
    )
    await restarted.restore_pending()

    assert restored == queued


@async_timeout()
async def test_runtime_builds_main_and_allowlisted_hip3_trade_subscriptions(
    engine, settings, clock
):
    settings.hyperliquid.hip3_dexes = ["xyz"]
    health = FakeHealth()
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        health,
        engine=engine,
        sink=None,
        clock=clock,
    )
    original_rest = runtime.rest

    class Rest:
        async def info(self, request_type: str, **params):
            if request_type == "perpDexs":
                return [None, {"name": "xyz"}]
            if params.get("dex") == "xyz":
                return {"universe": [{"name": "xyz:GOLD"}]}
            return {"universe": [{"name": "BTC"}, {"name": "ETH"}]}

    runtime.rest = Rest()
    try:
        await runtime._refresh_trade_subscriptions()
        async with engine.connect() as conn:
            cursor = await conn.scalar(
                select(hl_state.c.cursor).where(
                    hl_state.c.name == "trade_subscriptions"
                )
            )
    finally:
        runtime.rest = original_rest
        await runtime.close_resources()

    assert runtime.ws.subscription_count == 4
    subscriptions = runtime.ws._connections["trades"].subscriptions.values()
    assert {item["coin"] for item in subscriptions} == {"BTC", "ETH", "xyz:GOLD"}
    assert cursor["coins"] == ["BTC", "ETH", "xyz:GOLD"]


@async_timeout()
async def test_runtime_rejects_trade_set_that_would_exceed_global_subscription_limit(
    engine, settings, clock
):
    health = FakeHealth()
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        health,
        engine=engine,
        sink=None,
        clock=clock,
    )
    original_rest = runtime.rest

    class Rest:
        async def info(self, request_type: str, **params):
            if request_type == "perpDexs":
                return [None]
            return {"universe": [{"name": f"COIN-{index}"} for index in range(1_000)]}

    runtime.rest = Rest()
    try:
        with pytest.raises(ValueError, match="总订阅最多 1,000 个"):
            await runtime._refresh_trade_subscriptions()
    finally:
        runtime.rest = original_rest
        await runtime.close_resources()

    assert runtime.ws.subscription_count == 1
    assert health.failures and health.failures[-1][0] == "hl_trades_ws"


@async_timeout()
async def test_core_subscription_growth_is_rejected_atomically_at_global_limit(
    engine, settings, clock
):
    address = "0x1111111111111111111111111111111111111111"
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=address, tier="core", source="manual")
        )
    health = FakeHealth()
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        health,
        engine=engine,
        sink=None,
        clock=clock,
    )
    runtime.ws.add_connection(
        "trades", handler=runtime._handle_trade_ws_message, health_source="hl_trades_ws"
    )
    for index in range(998):
        coin = f"COIN-{index}"
        runtime.ws.add_subscription("trades", {"type": "trades", "coin": coin})
        runtime._trade_coins.add(coin)
    try:
        with pytest.raises(ValueError, match="总订阅最多 1,000 个"):
            await runtime.sync_core_subscriptions()
    finally:
        await runtime.close_resources()

    assert runtime.ws.subscription_count == 999
    assert runtime.core_addresses == set()
    assert health.failures and health.failures[-1][0] == "hl_core_ws"


@async_timeout()
async def test_core_wallet_can_be_replaced_when_subscriptions_are_exactly_at_limit(
    engine, settings, clock
):
    previous = "0x1111111111111111111111111111111111111111"
    replacement = "0x2222222222222222222222222222222222222222"
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=replacement,
                tier="core",
                source="manual",
            )
        )
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        FakeHealth(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    runtime.ws.add_connection(
        "trades", handler=runtime._handle_trade_ws_message, health_source="hl_trades_ws"
    )
    for index in range(997):
        coin = f"COIN-{index}"
        runtime.ws.add_subscription("trades", {"type": "trades", "coin": coin})
        runtime._trade_coins.add(coin)
    runtime.ws.add_subscription("core", {"type": "userFills", "user": previous})
    runtime.ws.add_subscription(
        "core",
        {"type": "allDexsClearinghouseState", "user": previous},
    )
    runtime._core_addresses = {previous}
    try:
        await runtime.sync_core_subscriptions()
    finally:
        await runtime.close_resources()

    assert runtime.ws.subscription_count == 1_000
    assert runtime.core_addresses == {replacement}
    subscriptions = runtime.ws._connections["core"].subscriptions.values()
    assert {item.get("user") for item in subscriptions if "user" in item} == {
        replacement
    }


@async_timeout()
async def test_local_trade_ws_replay_disconnects_and_resubscribes(engine, settings, clock):
    fixture = json.loads((FIXTURES / "trades_ws_sample.json").read_text(encoding="utf-8"))
    connections: list[list[dict]] = []
    replayed = 0
    reconnected = asyncio.Event()

    async def server_handler(websocket):
        nonlocal replayed
        received = [
            json.loads(await asyncio.wait_for(websocket.recv(), timeout=2))
            for _ in range(2)
        ]
        connections.append(received)
        await websocket.send(json.dumps(fixture))
        replayed += 1
        if replayed == 1:
            await websocket.close()
        else:
            reconnected.set()
            await websocket.wait_closed()

    discovery = TradeDiscovery(
        settings.hyperliquid,
        engine=engine,
        registry=WalletRegistry(engine, clock=clock),
        monitor=None,
        rest=None,
        sink=None,
        clock=clock,
    )
    health = FakeHealth()
    async with serve(server_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]

        async def fast_sleep(seconds: float) -> None:
            if seconds == 3600:
                await asyncio.Event().wait()
            await asyncio.sleep(0)

        manager = HyperliquidWsManager(
            url=f"ws://127.0.0.1:{port}",
            proxy_mode="direct",
            health=health,
            health_source="hl_trades_ws",
            sleep=fast_sleep,
            ping_interval_s=3600,
        )
        manager.add_connection(
            "trades", handler=discovery.handle_message, health_source="hl_trades_ws"
        )
        manager.add_subscription("trades", {"type": "trades", "coin": "BTC"})
        manager.add_subscription("trades", {"type": "trades", "coin": "xyz:GOLD"})
        task = asyncio.create_task(manager.run())
        try:
            await asyncio.wait_for(reconnected.wait(), timeout=3)
        finally:
            await manager.aclose()
            await asyncio.wait_for(task, timeout=3)

    assert len(connections) == 2
    assert connections[0] == connections[1]
    assert any(source == "hl_trades_ws" for source, _ in health.failures)
    assert health.successes.count("hl_trades_ws") >= 2


@async_timeout()
async def test_discovery_query_cap_leaves_ready_hot_reconcile_runnable(
    engine, settings, clock
):
    now = 100.0
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        FakeHealth(),
        engine=engine,
        sink=None,
        clock=clock,
        monotonic=lambda: now,
    )
    discovery_calls: list[str] = []
    reconcile_calls: list[str] = []

    async def qualify(request: QualificationRequest) -> bool:
        discovery_calls.append(request.address)
        return True

    async def reconcile(address: str) -> bool:
        reconcile_calls.append(address)
        return True

    runtime.discovery.process_qualification = qualify
    runtime.monitor.reconcile = reconcile
    deferred = 0

    def record_deferral() -> None:
        nonlocal deferred
        deferred += 1

    runtime.discovery.record_query_limit_deferral = record_deferral
    for index in range(4):
        runtime._schedule_discovery_query(
            QualificationRequest(
                address=f"0x{index + 1:040x}",
                coin="BTC",
                start_ms=0,
                end_ms=3_000,
                notional_usd=Decimal("5000000"),
                trade_count=1,
            )
        )
    hot = "0x9999999999999999999999999999999999999999"
    runtime.reconcile_queue.schedule(
        hot,
        ReconcilePriority.LIQUIDATION,
        due_at=now,
        reason="接近强平钱包对账",
    )
    try:
        for _ in range(settings.hyperliquid.discovery_query_limit_per_min + 2):
            assert await runtime.run_reconcile_once(now)
    finally:
        await runtime.close_resources()

    assert len(discovery_calls) == settings.hyperliquid.discovery_query_limit_per_min
    assert reconcile_calls == [hot]
    assert len(runtime._pending_discoveries) == 1
    assert deferred == 1
