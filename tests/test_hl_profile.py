from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest
from sqlalchemy import func, insert, select, update

from fakes import FakeHealth, async_timeout
from tidewatch.hyperliquid.monitor import HyperliquidMonitor
from tidewatch.hyperliquid.profile import (
    FillCoverage,
    ProfileBatchRunner,
    WalletProfiler,
    calculate_wallet_profile,
    classify_style,
    estimate_batch_duration,
    rebuild_holding_durations,
    seconds_until_shanghai_midnight,
)
from tidewatch.hyperliquid.runtime import HyperliquidRuntime
from tidewatch.store.tables import hl_fills, hl_state, hl_wallets


ADDRESS = "0x1111111111111111111111111111111111111111"
OTHER = "0x2222222222222222222222222222222222222222"
NOW = datetime(2026, 10, 6, 16, 0, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def _fill(
    tid: int,
    *,
    coin: str,
    hours: float,
    start: str,
    size: str,
    side: str,
    closed_pnl: str = "0",
    crossed: bool = True,
    price: str = "100",
) -> dict:
    return {
        "coin": coin,
        "px": price,
        "sz": size,
        "side": side,
        "time": int((NOW - timedelta(days=2) + timedelta(hours=hours)).timestamp() * 1000),
        "startPosition": start,
        "dir": (
            "Close Long"
            if side == "A" and Decimal(start) == Decimal(size)
            else "Open Long"
        ),
        "closedPnl": closed_pnl,
        "hash": f"0x{tid:064x}",
        "crossed": crossed,
        "fee": "1",
        "tid": tid,
    }


def _portfolio() -> list:
    return [
        [
            "month",
            {
                "accountValueHistory": [[1, "2000"], [2, "2030"]],
                "pnlHistory": [[1, "10"], [2, "40"]],
                "vlm": "300",
            },
        ],
        [
            "allTime",
            {
                "accountValueHistory": [[1, "1000"], [2, "1100"], [3, "1050"]],
                "pnlHistory": [[1, "0"], [2, "100"], [3, "40"]],
                "vlm": "1000",
            },
        ],
    ]


def _round_fills() -> list[dict]:
    return [
        _fill(1, coin="BTC", hours=0, start="0", size="1", side="B", price="100"),
        _fill(
            2,
            coin="BTC",
            hours=6,
            start="1",
            size="1",
            side="A",
            closed_pnl="100",
            price="110",
        ),
        _fill(3, coin="ETH", hours=1, start="0", size="2", side="B", price="50"),
        _fill(
            4,
            coin="ETH",
            hours=13,
            start="2",
            size="2",
            side="A",
            closed_pnl="50",
            price="55",
        ),
        _fill(5, coin="SOL", hours=2, start="0", size="3", side="B", price="20"),
        _fill(
            6,
            coin="SOL",
            hours=20,
            start="3",
            size="3",
            side="A",
            closed_pnl="-50",
            price="10",
        ),
        _fill(7, coin="HYPE", hours=21, start="0", size="4", side="B", price="5"),
    ]


def test_recorded_profile_fixtures_cover_protocol_edges_without_network():
    portfolio = json.loads((FIXTURES / "portfolio_profile.json").read_text())
    empty = json.loads((FIXTURES / "user_fills_by_time_empty.json").read_text())
    first = json.loads((FIXTURES / "user_fills_by_time_page_1.json").read_text())
    second = json.loads((FIXTURES / "user_fills_by_time_page_2.json").read_text())
    rounds = json.loads((FIXTURES / "user_fills_holding_rounds.json").read_text())
    abstraction = json.loads((FIXTURES / "user_abstraction_unified.json").read_text())
    spot = json.loads((FIXTURES / "spot_clearinghouse_state_unified.json").read_text())
    manifest = json.loads((FIXTURES / "profile_samples_manifest.json").read_text())

    assert {item[0] for item in portfolio} == {
        "day",
        "week",
        "month",
        "allTime",
        "perpDay",
        "perpWeek",
        "perpMonth",
        "perpAllTime",
    }
    assert all(item[1]["pnlHistory"] and item[1]["accountValueHistory"] for item in portfolio)
    assert all("vlm" in item[1] for item in portfolio)
    assert empty == []
    assert first[-1]["time"] < second[0]["time"]
    assert any(fill["startPosition"] == "0.0" for fill in rounds)
    assert abstraction == "unifiedAccount"
    assert next(item for item in spot["balances"] if item["coin"] == "USDC")["total"]
    assert manifest["high_frequency_short_window"]["original_count"] == 2000


def test_metric_example_matches_hand_calculation_and_discloses_every_sample():
    fills = _round_fills()
    result = calculate_wallet_profile(
        portfolio=_portfolio(),
        fills=fills,
        clearing_state={
            "marginSummary": {"accountValue": "2000", "totalNtlPos": "5000"},
            "assetPositions": [
                {"position": {"coin": "BTC", "leverage": {"value": 5}}},
                {"position": {"coin": "ETH", "leverage": {"value": 10}}},
            ],
        },
        abstraction="unifiedAccount",
        spot_state={"balances": [{"coin": "USDC", "total": "500"}]},
        coverage=FillCoverage(
            start_at=NOW - timedelta(days=2),
            end_at=NOW,
            complete=True,
            sample_size=len(fills),
        ),
        now=NOW,
    )

    profile = result.profile
    assert profile["account_value"] == "2500"
    assert profile["total_pnl"] == "40"
    assert profile["pnl_30d"] == "30"
    assert profile["pnl_history"] == [[1, "0"], [2, "100"], [3, "40"]]
    assert profile["volume_total"] == "1000"
    assert profile["volume_30d"] == "300"
    assert profile["max_drawdown"] == "60"
    assert Decimal(profile["max_drawdown_ratio"]) == Decimal("60") / Decimal("1100")
    assert profile["win_rate"] == pytest.approx(2 / 3)
    assert profile["profit_loss_ratio"] == "1.5"
    assert profile["holding_duration_avg_s"] == 12 * 3600
    assert profile["holding_duration_median_s"] == 12 * 3600
    assert profile["open_rounds"] == 1
    assert profile["daily_fill_count"] == 3.5
    assert profile["actual_leverage"] == "2"
    assert profile["configured_leverage"] == {"BTC": "5", "ETH": "10"}
    assert [item["coin"] for item in profile["preferred_coins"]] == [
        "BTC",
        "ETH",
        "SOL",
        "HYPE",
    ]
    assert result.style == "directional"
    assert set(profile["coverage"]) == {
        "account_value",
        "total_pnl",
        "pnl_30d",
        "volume_total",
        "volume_30d",
        "max_drawdown",
        "win_rate",
        "profit_loss_ratio",
        "holding_duration",
        "preferred_coins",
        "daily_fill_count",
        "actual_leverage",
        "configured_leverage",
    }
    assert profile["coverage"]["win_rate"]["sample_size"] == 3
    assert profile["coverage"]["holding_duration"]["sample_size"] == 3


def test_empty_history_uses_null_and_explicit_insufficient_markers():
    result = calculate_wallet_profile(
        portfolio=[],
        fills=[],
        clearing_state={
            "marginSummary": {"accountValue": "0", "totalNtlPos": "0"},
            "assetPositions": [],
        },
        abstraction="disabled",
        spot_state=None,
        coverage=FillCoverage(NOW - timedelta(days=30), NOW, True, 0),
        now=NOW,
    )

    assert result.style == "unknown"
    assert result.profile["total_pnl"] is None
    assert result.profile["win_rate"] is None
    assert result.profile["holding_duration_median_s"] is None
    assert result.profile["coverage"]["total_pnl"]["sufficient"] is False
    assert result.profile["coverage"]["win_rate"]["reason"] == "没有平仓类成交"
    assert result.profile["coverage"]["holding_duration"]["reason"] == "没有完整持仓轮次"


def test_old_fills_without_position_fields_make_holding_metric_explicitly_incomplete():
    fills = _round_fills()
    fills.append(
        {
            "coin": "BTC",
            "px": "100",
            "sz": "1",
            "time": int(NOW.timestamp() * 1000),
            "dir": "Close Long",
            "closedPnl": "1",
            "crossed": True,
            "tid": 99,
        }
    )

    result = calculate_wallet_profile(
        portfolio=_portfolio(),
        fills=fills,
        clearing_state={
            "marginSummary": {"accountValue": "100", "totalNtlPos": "0"},
            "assetPositions": [],
        },
        abstraction="disabled",
        spot_state=None,
        coverage=FillCoverage(NOW - timedelta(days=30), NOW, True, len(fills)),
        now=NOW,
    )

    assert result.profile["holding_duration_avg_s"] is None
    assert result.profile["holding_duration_median_s"] is None
    assert result.profile["coverage"]["holding_duration"] == {
        "start_at": (NOW - timedelta(days=30)).isoformat(),
        "end_at": NOW.isoformat(),
        "sample_size": 3,
        "complete": False,
        "sufficient": False,
        "reason": "1 笔成交缺少 side 或 startPosition，无法完整重建",
    }


def test_leverage_and_account_value_include_configured_hip3_dexes():
    result = calculate_wallet_profile(
        portfolio=_portfolio(),
        fills=[],
        clearing_state={
            "marginSummary": {"accountValue": "1000", "totalNtlPos": "2000"},
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "leverage": {"value": 2},
                        "positionValue": "2000",
                    }
                },
            ],
        },
        dex_clearing_states={
            "xyz": {
                "marginSummary": {"accountValue": "500", "totalNtlPos": "1000"},
                "assetPositions": [
                    {
                        "position": {
                            "coin": "xyz:SILVER",
                            "leverage": {"value": 4},
                            "positionValue": "1000",
                        }
                    },
                ],
            }
        },
        abstraction="unifiedAccount",
        spot_state={"balances": [{"coin": "USDC", "total": "500"}]},
        coverage=FillCoverage(NOW - timedelta(days=30), NOW, True, 0),
        now=NOW,
    )

    assert result.profile["account_value"] == "2000"
    assert result.profile["actual_leverage"] == "1.5"
    assert result.profile["configured_leverage"] == {"BTC": "2", "xyz:SILVER": "4"}
    assert result.profile["configured_leverage_notionals"] == {
        "BTC": "2000",
        "xyz:SILVER": "1000",
    }
    assert result.profile["coverage"]["account_value"]["sample_size"] == 3


def test_drawdown_amount_uses_pnl_peak_and_ratio_uses_that_peaks_account_value():
    portfolio = [
        [
            "allTime",
            {
                "accountValueHistory": [
                    [1, "10000"],
                    [2, "9900"],
                    [3, "100"],
                    [4, "10"],
                ],
                "pnlHistory": [[1, "100"], [2, "0"], [3, "90"], [4, "0"]],
                "vlm": "0",
            },
        ]
    ]
    result = calculate_wallet_profile(
        portfolio=portfolio,
        fills=[],
        clearing_state={
            "marginSummary": {"accountValue": "1", "totalNtlPos": "0"},
            "assetPositions": [],
        },
        abstraction="disabled",
        spot_state=None,
        coverage=FillCoverage(NOW - timedelta(days=30), NOW, True, 0),
        now=NOW,
    )

    assert result.profile["max_drawdown"] == "100"
    assert result.profile["max_drawdown_ratio"] == "0.01"


def test_drawdown_aligns_nearest_earlier_account_value_when_pnl_peak_is_near_zero():
    portfolio = [
        [
            "allTime",
            {
                "accountValueHistory": [[100, "500"], [250, "400"]],
                "pnlHistory": [[100, "-1"], [200, "0.01"], [300, "-100"]],
                "vlm": "0",
            },
        ]
    ]

    result = calculate_wallet_profile(
        portfolio=portfolio,
        fills=[],
        clearing_state={
            "marginSummary": {"accountValue": "1", "totalNtlPos": "0"},
            "assetPositions": [],
        },
        abstraction="disabled",
        spot_state=None,
        coverage=FillCoverage(NOW - timedelta(days=30), NOW, True, 0),
        now=NOW,
    )

    assert result.profile["max_drawdown"] == "100.01"
    assert result.profile["max_drawdown_ratio"] == "0.20002"


@pytest.mark.parametrize(
    ("accounts", "reason"),
    [
        ([[250, "500"]], "回撤起点前没有可对齐的账户价值"),
        ([[100, "0"]], "回撤起点对应的账户价值小于等于零"),
    ],
)
def test_drawdown_keeps_amount_and_explains_unavailable_ratio(accounts, reason):
    portfolio = [
        [
            "allTime",
            {
                "accountValueHistory": accounts,
                "pnlHistory": [[100, "10"], [200, "-20"]],
                "vlm": "0",
            },
        ]
    ]

    result = calculate_wallet_profile(
        portfolio=portfolio,
        fills=[],
        clearing_state={
            "marginSummary": {"accountValue": "1", "totalNtlPos": "0"},
            "assetPositions": [],
        },
        abstraction="disabled",
        spot_state=None,
        coverage=FillCoverage(NOW - timedelta(days=30), NOW, True, 0),
        now=NOW,
    )

    assert result.profile["max_drawdown"] == "30"
    assert result.profile["max_drawdown_ratio"] is None
    assert result.profile["coverage"]["max_drawdown"]["sufficient"] is False
    assert result.profile["coverage"]["max_drawdown"]["reason"] == reason


def test_holding_rounds_only_count_complete_zero_to_position_to_zero_cycles():
    durations, open_rounds = rebuild_holding_durations(_round_fills())

    assert durations == [6 * 3600, 12 * 3600, 18 * 3600]
    assert open_rounds == 1


@pytest.mark.parametrize(
    ("passive_ratio", "daily", "median", "fills", "rounds", "expected"),
    [
        (0.8001, 1000.1, 1, 20, 0, "high_frequency"),
        (0.8, 1001, 13 * 3600, 20, 2, "swing"),
        (0.9, 1000, 12 * 3600, 20, 2, "directional"),
        (0.2, 1, 12 * 3600 + 1, 20, 2, "swing"),
        (0.2, 1, 12 * 3600, 20, 2, "directional"),
        (0.9, 2000, None, 19, 0, "unknown"),
        (0.2, 1, None, 20, 1, "unknown"),
    ],
)
def test_style_boundaries_and_insufficient_samples(
    passive_ratio, daily, median, fills, rounds, expected
):
    assert classify_style(passive_ratio, daily, median, fills, rounds) == expected


def test_schedule_and_duration_estimate_use_sharded_candidate_strategy():
    exact = datetime(2026, 10, 6, 16, 0, tzinfo=timezone.utc)  # 上海 00:00
    before = exact - timedelta(minutes=10)

    assert seconds_until_shanghai_midnight(exact) == 0
    assert seconds_until_shanghai_midnight(before) == 600
    estimate = estimate_batch_duration(
        focus_count=200,
        candidate_count=1000,
        candidate_shards=2,
        weight_per_wallet=62,
        budget_per_minute=200,
    )
    assert estimate.wallet_count == 700
    assert estimate.weight == 43_400
    assert estimate.minutes == pytest.approx(217)


@async_timeout()
async def test_runtime_wires_real_profiler_and_batch_configuration(engine, settings, clock):
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        FakeHealth(),
        engine=engine,
        clock=clock,
    )
    try:
        assert runtime.profiler.rest is runtime.rest
        assert runtime.profile_batch.profiler is runtime.profiler
        assert runtime.profile_batch.retry_limit == settings.hyperliquid.profile_retry_limit
        assert runtime.profile_batch.candidate_shards == settings.hyperliquid.profile_candidate_shards
        assert settings.hyperliquid.frequencies.profile_initial_delay_s == 600
        assert settings.hyperliquid.health.hl_profile.stale_after_s >= 27 * 3600
    finally:
        await runtime.close_resources()


class _PagedRest:
    def __init__(self, pages: list[list[dict]]) -> None:
        self.pages = list(pages)
        self.calls: list[tuple[str, str, dict]] = []

    async def info(self, request_type: str, *, budget: str, **params):
        self.calls.append((request_type, budget, params))
        if request_type == "userFillsByTime":
            return self.pages.pop(0)
        raise AssertionError(f"未预期的请求：{request_type}")


@async_timeout()
async def test_incremental_fill_pages_restart_and_overlap_are_idempotent(engine, clock):
    first = _fill(101, coin="BTC", hours=1, start="0", size="1", side="B")
    second = _fill(102, coin="BTC", hours=2, start="1", size="1", side="A")
    third = _fill(103, coin="ETH", hours=3, start="0", size="1", side="B")
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS, tier="candidate", source="leaderboard", excluded=False
            )
        )

    profiler = WalletProfiler(
        engine,
        _PagedRest([[first, second], [second, third], []]),
        clock=clock,
        fill_page_size=2,
    )
    coverage = await profiler.sync_fills(
        ADDRESS,
        start_at=NOW - timedelta(days=30),
        end_at=NOW,
    )
    assert coverage.sample_size == 3 and coverage.complete is True

    restarted = WalletProfiler(
        engine,
        _PagedRest([[second, third], []]),
        clock=clock,
        fill_page_size=2,
    )
    await restarted.sync_fills(ADDRESS, start_at=NOW - timedelta(days=30), end_at=NOW)
    async with engine.connect() as conn:
        count = await conn.scalar(select(func.count()).select_from(hl_fills))
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"profile_fills:{ADDRESS}")
        )
    assert count == 3
    assert cursor["last_time"] == third["time"]


@async_timeout()
async def test_short_increment_does_not_hide_previous_fill_history_gap(engine, clock):
    pages = [
        [
            _fill(201, coin="BTC", hours=1, start="0", size="1", side="B"),
            _fill(202, coin="BTC", hours=2, start="1", size="1", side="A"),
        ],
        [
            _fill(203, coin="ETH", hours=3, start="0", size="1", side="B"),
            _fill(204, coin="ETH", hours=4, start="1", size="1", side="A"),
        ],
    ]
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS, tier="candidate", source="leaderboard", excluded=False
            )
        )

    first = WalletProfiler(
        engine,
        _PagedRest(pages),
        clock=clock,
        fill_page_size=2,
        fill_max_pages=2,
    )
    first_coverage = await first.sync_fills(
        ADDRESS,
        start_at=NOW - timedelta(days=30),
        end_at=NOW,
    )
    assert first_coverage.complete is False

    increment = _fill(205, coin="SOL", hours=5, start="0", size="1", side="B")
    second = WalletProfiler(
        engine,
        _PagedRest([[increment]]),
        clock=clock,
        fill_page_size=2,
        fill_max_pages=2,
    )
    second_coverage = await second.sync_fills(
        ADDRESS,
        start_at=NOW - timedelta(days=29),
        end_at=NOW + timedelta(days=1),
    )

    assert second_coverage.complete is False
    assert second_coverage.start_at == datetime.fromtimestamp(
        pages[0][0]["time"] / 1000,
        tz=timezone.utc,
    )
    async with engine.connect() as conn:
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"profile_fills:{ADDRESS}")
        )
    assert cursor["incomplete_before"] == pages[0][0]["time"]


@async_timeout()
async def test_new_history_truncation_replaces_an_expired_old_gap(engine, clock):
    def timed_fill(tid: int, when: datetime) -> dict:
        fill = _fill(tid, coin="BTC", hours=1, start="0", size="1", side="B")
        fill["time"] = int(when.timestamp() * 1000)
        return fill

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS, tier="candidate", source="leaderboard", excluded=False
            )
        )
    first_pages = [
        [timed_fill(401, NOW - timedelta(days=2)), timed_fill(402, NOW - timedelta(days=2, hours=-1))],
        [timed_fill(403, NOW - timedelta(days=2, hours=-2)), timed_fill(404, NOW - timedelta(days=2, hours=-3))],
    ]
    await WalletProfiler(
        engine,
        _PagedRest(first_pages),
        clock=clock,
        fill_page_size=2,
        fill_max_pages=2,
    ).sync_fills(ADDRESS, start_at=NOW - timedelta(days=30), end_at=NOW)

    new_gap_start = NOW + timedelta(days=2)
    second_pages = [
        [timed_fill(405, new_gap_start), timed_fill(406, new_gap_start + timedelta(hours=1))],
        [timed_fill(407, new_gap_start + timedelta(hours=2)), timed_fill(408, new_gap_start + timedelta(hours=3))],
    ]
    await WalletProfiler(
        engine,
        _PagedRest(second_pages),
        clock=clock,
        fill_page_size=2,
        fill_max_pages=2,
    ).sync_fills(
        ADDRESS,
        start_at=NOW + timedelta(days=1),
        end_at=NOW + timedelta(days=3),
    )
    increment = timed_fill(409, NOW + timedelta(days=3, hours=12))
    coverage = await WalletProfiler(
        engine,
        _PagedRest([[increment]]),
        clock=clock,
        fill_page_size=2,
        fill_max_pages=2,
    ).sync_fills(
        ADDRESS,
        start_at=NOW + timedelta(days=1, hours=12),
        end_at=NOW + timedelta(days=4),
    )

    assert coverage.complete is False
    async with engine.connect() as conn:
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"profile_fills:{ADDRESS}")
        )
    assert cursor["incomplete_before"] == int(new_gap_start.timestamp() * 1000)


@async_timeout()
async def test_fill_pagination_overlaps_last_millisecond_without_losing_new_tid(engine, clock):
    same_time = int((NOW - timedelta(hours=1)).timestamp() * 1000)
    first = _fill(501, coin="BTC", hours=1, start="0", size="1", side="B")
    boundary = _fill(502, coin="BTC", hours=2, start="1", size="1", side="A")
    overlap = _fill(503, coin="ETH", hours=2, start="0", size="1", side="B")
    later = _fill(504, coin="ETH", hours=3, start="1", size="1", side="A")
    boundary["time"] = same_time
    overlap["time"] = same_time
    later["time"] = same_time + 1
    rest = _PagedRest([[first, boundary], [overlap, later], []])
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS, tier="candidate", source="leaderboard", excluded=False
            )
        )
    profiler = WalletProfiler(engine, rest, clock=clock, fill_page_size=2)

    coverage = await profiler.sync_fills(
        ADDRESS,
        start_at=NOW - timedelta(days=30),
        end_at=NOW,
    )

    assert coverage.sample_size == 4
    assert rest.calls[1][2]["startTime"] == same_time


@async_timeout()
async def test_fill_pagination_duplicate_only_short_page_marks_history_complete(engine, clock):
    first = _fill(511, coin="BTC", hours=1, start="0", size="1", side="B")
    boundary = _fill(512, coin="BTC", hours=2, start="1", size="1", side="A")
    rest = _PagedRest([[first, boundary], [boundary]])
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS, tier="candidate", source="leaderboard", excluded=False
            )
        )
    profiler = WalletProfiler(engine, rest, clock=clock, fill_page_size=2)

    coverage = await profiler.sync_fills(
        ADDRESS,
        start_at=NOW - timedelta(days=30),
        end_at=NOW,
    )

    assert coverage.complete is True
    assert coverage.sample_size == 2


@async_timeout()
async def test_monitor_and_profile_rest_overlap_share_fill_idempotency(engine, settings, clock):
    fill = _fill(301, coin="BTC", hours=1, start="0", size="1", side="B")
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS, tier="core", source="manual", excluded=False
            )
        )
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=None,
        clock=clock,
    )
    await monitor._store_fills(ADDRESS, [(fill, False)])
    profiler = WalletProfiler(
        engine,
        _PagedRest([[fill]]),
        clock=clock,
        fill_page_size=2,
    )

    await profiler.sync_fills(
        ADDRESS,
        start_at=NOW - timedelta(days=30),
        end_at=NOW,
    )

    async with engine.connect() as conn:
        rows = (await conn.execute(select(hl_fills))).mappings().all()
    assert len(rows) == 1
    assert rows[0]["side"] == "B" and rows[0]["start_position"] == Decimal("0")


@async_timeout()
async def test_profile_backfills_old_fill_fields_with_batch_pagination_before_calculation(
    engine, clock
):
    old_fills = [
        _fill(601, coin="BTC", hours=1, start="0", size="1", side="B"),
        _fill(602, coin="BTC", hours=2, start="1", size="1", side="B"),
        _fill(603, coin="BTC", hours=7, start="2", size="2", side="A"),
    ]
    base_time = clock() - timedelta(days=2)
    for fill, hours in zip(old_fills, (1, 2, 7), strict=True):
        fill["time"] = int((base_time + timedelta(hours=hours)).timestamp() * 1000)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS, tier="candidate", source="leaderboard", excluded=False
            )
        )
        await conn.execute(
            insert(hl_fills),
            [
                {
                    "address": ADDRESS,
                    "tid": fill["tid"],
                    "coin": fill["coin"],
                    "dir": f"旧方向 {fill['tid']}",
                    "side": None,
                    "start_position": None,
                    "px": Decimal(fill["px"]),
                    "sz": Decimal(fill["sz"]),
                    "closed_pnl": Decimal(fill["closedPnl"]),
                    "fee": Decimal("9"),
                    "crossed": fill["crossed"],
                    "occurred_at": datetime.fromtimestamp(
                        fill["time"] / 1000, tz=timezone.utc
                    ),
                    "hash": fill["hash"],
                    "is_twap": False,
                    "liquidation": None,
                }
                for fill in old_fills
            ],
        )

    class Rest:
        def __init__(self):
            self.fill_calls: list[dict] = []

        async def info(self, request_type: str, *, budget: str, **params):
            assert budget == "batch"
            if request_type == "userFillsByTime":
                self.fill_calls.append(params)
                pages = [[], old_fills[:2], old_fills[1:], []]
                return pages[len(self.fill_calls) - 1]
            if request_type == "portfolio":
                return _portfolio()
            if request_type == "userAbstraction":
                return "disabled"
            if request_type == "clearinghouseState":
                return {
                    "marginSummary": {"accountValue": "100", "totalNtlPos": "0"},
                    "assetPositions": [],
                }
            raise AssertionError(request_type)

    rest = Rest()
    profiler = WalletProfiler(engine, rest, clock=clock, fill_page_size=2)

    result = await profiler.profile_wallet(ADDRESS)

    assert len(rest.fill_calls) == 4
    assert rest.fill_calls[1]["startTime"] == old_fills[0]["time"]
    assert rest.fill_calls[1]["endTime"] == old_fills[-1]["time"]
    assert rest.fill_calls[2]["startTime"] == old_fills[1]["time"]
    assert result["profile"]["holding_duration_avg_s"] == 6 * 3600
    assert result["profile"]["coverage"]["holding_duration"]["sufficient"] is True
    async with engine.connect() as conn:
        rows = (
            await conn.execute(select(hl_fills).order_by(hl_fills.c.tid))
        ).mappings().all()
    assert len(rows) == 3
    assert [(row["side"], row["start_position"]) for row in rows] == [
        ("B", Decimal("0")),
        ("B", Decimal("1")),
        ("A", Decimal("2")),
    ]
    assert [row["dir"] for row in rows] == ["旧方向 601", "旧方向 602", "旧方向 603"]
    assert [row["fee"] for row in rows] == [Decimal("9")] * 3


@async_timeout()
async def test_profile_marks_holding_data_insufficient_when_old_fill_backfill_fails(
    engine, clock
):
    fill = _fill(611, coin="BTC", hours=1, start="0", size="1", side="B")
    fill["time"] = int((clock() - timedelta(days=2)).timestamp() * 1000)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS, tier="candidate", source="leaderboard", excluded=False
            )
        )
        await conn.execute(
            insert(hl_fills).values(
                address=ADDRESS,
                tid=fill["tid"],
                coin=fill["coin"],
                dir=fill["dir"],
                side=None,
                start_position=None,
                px=Decimal(fill["px"]),
                sz=Decimal(fill["sz"]),
                closed_pnl=Decimal(fill["closedPnl"]),
                fee=Decimal(fill["fee"]),
                crossed=fill["crossed"],
                occurred_at=datetime.fromtimestamp(fill["time"] / 1000, tz=timezone.utc),
                hash=fill["hash"],
                is_twap=False,
            )
        )

    class Rest:
        def __init__(self):
            self.fill_calls = 0

        async def info(self, request_type: str, *, budget: str, **params):
            assert budget == "batch"
            if request_type == "userFillsByTime":
                self.fill_calls += 1
                if self.fill_calls == 1:
                    return []
                raise RuntimeError("补齐请求失败")
            if request_type == "portfolio":
                return _portfolio()
            if request_type == "userAbstraction":
                return "disabled"
            if request_type == "clearinghouseState":
                return {
                    "marginSummary": {"accountValue": "100", "totalNtlPos": "0"},
                    "assetPositions": [],
                }
            raise AssertionError(request_type)

    rest = Rest()
    result = await WalletProfiler(engine, rest, clock=clock).profile_wallet(ADDRESS)

    assert rest.fill_calls == 2
    assert result["profile"]["holding_duration_avg_s"] is None
    assert result["profile"]["coverage"]["holding_duration"]["reason"] == (
        "1 笔成交缺少 side 或 startPosition，无法完整重建"
    )


@async_timeout()
async def test_profile_save_merges_metadata_changed_during_long_fetch(engine, clock):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS,
                tier="candidate",
                source="leaderboard",
                excluded=False,
                profile={"leaderboard_display_name": "旧名称"},
            )
        )

    class Rest:
        def __init__(self):
            self.updated = False

        async def info(self, request_type: str, *, budget: str, **params):
            assert budget == "batch"
            if request_type == "userFillsByTime":
                return []
            if request_type == "portfolio":
                if not self.updated:
                    async with engine.begin() as conn:
                        await conn.execute(
                            update(hl_wallets)
                            .where(hl_wallets.c.address == ADDRESS)
                            .values(profile={"leaderboard_display_name": "新名称"})
                        )
                    self.updated = True
                return _portfolio()
            if request_type == "userAbstraction":
                return "disabled"
            if request_type == "clearinghouseState":
                return {
                    "marginSummary": {"accountValue": "100", "totalNtlPos": "0"},
                    "assetPositions": [],
                }
            raise AssertionError(request_type)

    profiler = WalletProfiler(engine, Rest(), clock=clock)
    await profiler.profile_wallet(ADDRESS)

    async with engine.connect() as conn:
        profile = await conn.scalar(
            select(hl_wallets.c.profile).where(hl_wallets.c.address == ADDRESS)
        )
    assert profile["leaderboard_display_name"] == "新名称"
    assert profile["account_value"] == "100"


@async_timeout()
async def test_profiler_queries_each_configured_hip3_dex_with_batch_budget(engine, clock):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS,
                tier="focus",
                source="manual",
                excluded=False,
            )
        )

    class Rest:
        def __init__(self):
            self.clearing_calls: list[dict] = []

        async def info(self, request_type: str, *, budget: str, **params):
            assert budget == "batch"
            if request_type == "userFillsByTime":
                return []
            if request_type == "portfolio":
                return _portfolio()
            if request_type == "userAbstraction":
                return "disabled"
            if request_type == "clearinghouseState":
                self.clearing_calls.append(params)
                if params.get("dex") == "xyz":
                    return {
                        "marginSummary": {"accountValue": "500", "totalNtlPos": "1000"},
                        "assetPositions": [],
                    }
                return {
                    "marginSummary": {"accountValue": "1000", "totalNtlPos": "2000"},
                    "assetPositions": [],
                }
            raise AssertionError(request_type)

    rest = Rest()
    profiler = WalletProfiler(engine, rest, clock=clock, hip3_dexes=("xyz",))
    result = await profiler.profile_wallet(ADDRESS)

    assert rest.clearing_calls == [{"user": ADDRESS}, {"user": ADDRESS, "dex": "xyz"}]
    assert result["profile"]["account_value"] == "1500"
    assert result["profile"]["actual_leverage"] == "2"
    async with engine.connect() as conn:
        metadata = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"account_metadata:{ADDRESS}"
            )
        )
    assert metadata["account_type"] == "disabled"
    assert metadata["account_type_updated_at"] == clock.now.isoformat()


@async_timeout()
async def test_candidate_profile_does_not_persist_monitoring_account_metadata(
    engine, clock
):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS,
                tier="candidate",
                source="leaderboard",
                excluded=False,
            )
        )

    class Rest:
        async def info(self, request_type: str, *, budget: str, **params):
            assert budget == "batch"
            if request_type == "userFillsByTime":
                return []
            if request_type == "portfolio":
                return _portfolio()
            if request_type == "userAbstraction":
                return "disabled"
            if request_type == "clearinghouseState":
                return {
                    "marginSummary": {"accountValue": "1000", "totalNtlPos": "0"},
                    "assetPositions": [],
                }
            raise AssertionError(request_type)

    await WalletProfiler(engine, Rest(), clock=clock).profile_wallet(ADDRESS)

    async with engine.connect() as conn:
        metadata = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"account_metadata:{ADDRESS}"
            )
        )
    assert metadata is None


class _FakeProfiler:
    def __init__(self, failures: dict[str, int] | None = None) -> None:
        self.failures = dict(failures or {})
        self.calls: list[str] = []

    async def profile_wallet(self, address: str):
        self.calls.append(address)
        remaining = self.failures.get(address, 0)
        if remaining:
            self.failures[address] = remaining - 1
            raise RuntimeError("画像失败")
        return {"address": address}


@async_timeout()
async def test_batch_prioritizes_focus_shards_candidates_and_isolates_failure(engine, clock):
    focus = "0x0000000000000000000000000000000000000010"
    candidates = [f"0x{number:040x}" for number in range(20, 26)]
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {"address": focus, "tier": "focus", "source": "manual", "excluded": False},
                *[
                    {
                        "address": address,
                        "tier": "candidate",
                        "source": "leaderboard",
                        "excluded": False,
                    }
                    for address in candidates
                ],
            ],
        )
    selected_date = date(2026, 10, 7)
    selected = [
        address
        for address in candidates
        if int(address[2:], 16) % 2 == selected_date.toordinal() % 2
    ]
    failing = selected[0]
    profiler = _FakeProfiler({failing: 9})
    health = FakeHealth()
    runner = ProfileBatchRunner(
        engine,
        profiler,
        health=health,
        clock=clock,
        retry_limit=2,
        candidate_shards=2,
        failure_ratio_threshold=0.5,
    )

    result = await runner.run_once(batch_date=selected_date)

    assert profiler.calls[0] == focus
    assert set(profiler.calls) == {focus, *selected}
    assert profiler.calls.count(failing) == 2
    assert result.processed == 1 + len(selected)
    assert result.failed == (failing,)
    assert health.successes == ["hl_profile"] and health.failures == []
    async with engine.connect() as conn:
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == "profile_batch")
        )
    assert cursor["phase"] == "complete"
    assert cursor["failed"] == [failing]


@async_timeout()
async def test_fill_page_commit_failure_does_not_advance_profile_cursor(
    engine, clock, monkeypatch
):
    """约束类测试：成交页与游标必须处于同一事务。"""
    old_time = int((clock.now - timedelta(hours=2)).timestamp() * 1000)
    new_fill = _fill(
        99,
        coin="BTC",
        hours=1,
        start="0",
        size="1",
        side="B",
    )

    class Rest:
        async def info(self, request_type: str, **params):
            assert request_type == "userFillsByTime"
            return [new_fill]

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_state).values(
                name=f"profile_fills:{ADDRESS}",
                cursor={"last_time": old_time},
                updated_at=clock.now,
            )
        )
    profiler = WalletProfiler(engine, Rest(), clock=clock)
    original = profiler._store_fill_page

    async def fail_after_write(conn, address, fills):
        await original(conn, address, fills)
        raise RuntimeError("模拟事务提交前失败")

    monkeypatch.setattr(profiler, "_store_fill_page", fail_after_write)
    with pytest.raises(RuntimeError, match="模拟事务提交前失败"):
        await profiler.sync_fills(
            ADDRESS,
            start_at=clock.now - timedelta(days=1),
            end_at=clock.now,
        )

    async with engine.connect() as conn:
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"profile_fills:{ADDRESS}"
            )
        )
        count = await conn.scalar(select(func.count()).select_from(hl_fills))
    assert cursor["last_time"] == old_time
    assert count == 0


@async_timeout()
async def test_empty_batch_reports_real_success(engine, clock):
    health = FakeHealth()
    runner = ProfileBatchRunner(
        engine,
        _FakeProfiler(),
        health=health,
        clock=clock,
    )

    result = await runner.run_once(batch_date=date(2026, 10, 7))

    assert result.processed == 0 and result.succeeded == 0 and result.failed == ()
    assert health.successes == ["hl_profile"]


@async_timeout()
async def test_restart_after_midnight_runs_today_batch_when_only_yesterday_completed(
    engine, clock
):
    yesterday = clock().astimezone().date() - timedelta(days=1)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_state).values(
                name="profile_batch",
                cursor={
                    "phase": "complete",
                    "date": yesterday.isoformat(),
                    "addresses": [],
                    "next_index": 0,
                    "failed": [],
                },
                updated_at=clock(),
            )
        )
        await conn.execute(
            insert(hl_wallets).values(
                address=ADDRESS,
                tier="candidate",
                source="leaderboard",
                excluded=False,
                profile_updated_at=clock(),
            )
        )
    runner = ProfileBatchRunner(engine, _FakeProfiler(), health=FakeHealth(), clock=clock)

    assert await runner.initial_delay(600) == 0


@async_timeout()
async def test_unfinished_batch_resumes_original_date_after_midnight(engine, clock):
    addresses = [ADDRESS, OTHER]
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {"address": address, "tier": "focus", "source": "manual", "excluded": False}
                for address in addresses
            ],
        )
        await conn.execute(
            insert(hl_state).values(
                name="profile_batch",
                cursor={
                    "phase": "running",
                    "date": "2026-09-29",
                    "addresses": addresses,
                    "next_index": 1,
                    "failed": [],
                },
                updated_at=clock(),
            )
        )
    profiler = _FakeProfiler()
    runner = ProfileBatchRunner(engine, profiler, health=FakeHealth(), clock=clock)

    await runner.run_once()

    assert profiler.calls == [OTHER]
    async with engine.connect() as conn:
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == "profile_batch")
        )
    assert cursor["date"] == "2026-09-29" and cursor["phase"] == "complete"
