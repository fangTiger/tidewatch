"""M6 巨鲸网页查询与聚合口径。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import insert

from tidewatch.store.tables import events, hl_fills, hl_positions, hl_wallets
from tidewatch.web import queries as web_queries


NOW = datetime(2026, 10, 6, 4, 0, tzinfo=timezone.utc)
A = "0x1111111111111111111111111111111111111111"
B = "0x2222222222222222222222222222222222222222"
C = "0x3333333333333333333333333333333333333333"
D = "0x4444444444444444444444444444444444444444"


def _query(name: str):
    assert hasattr(web_queries, name), f"缺少 M6 查询函数 {name}"
    return getattr(web_queries, name)


def _profile(account: str, pnl: str, win_rate: float, *, complete: bool = True) -> dict:
    return {
        "account_value": account,
        "pnl_30d": pnl,
        "win_rate": win_rate,
        "total_pnl": "1234.5",
        "volume_total": "99999",
        "max_drawdown": "-300",
        "profit_loss_ratio": "1.8",
        "holding_duration_avg_s": 7200,
        "holding_duration_median_s": 3600,
        "preferred_coins": [{"coin": "BTC", "notional_usd": "1000"}],
        "daily_fill_count": 12.5,
        "passive_fill_ratio": 0.25,
        "actual_leverage": "2.5",
        "pnl_history": [[1000, "0"], [2000, pnl]],
        "coverage": {
            "pnl_30d": {
                "sufficient": True,
                "complete": complete,
                "reason": None if complete else "只有 12 天历史",
            }
        },
    }


async def _seed(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets), [
            {"address": A, "note": "方向鲸", "tier": "focus", "source": "manual", "excluded": False,
             "style": "directional", "profile": _profile("3000000", "500000", 0.75),
             "profile_updated_at": NOW - timedelta(hours=1)},
            {"address": B, "note": None, "tier": "core", "source": "leaderboard", "excluded": False,
             "style": "directional", "profile": {**_profile("5000000", "-10", 0.9),
                                                     "leaderboard_display_name": "榜单鲸"},
             "profile_updated_at": NOW - timedelta(hours=2)},
            {"address": C, "note": "候选", "tier": "candidate", "source": "discovered", "excluded": False,
             "style": "swing", "profile": _profile("9000000", "900000", 0.95),
             "profile_updated_at": NOW - timedelta(days=1)},
            {"address": D, "note": "已排除", "tier": None, "source": "manual", "excluded": True,
             "style": "directional", "profile": _profile("8000000", "800000", 0.99),
             "profile_updated_at": NOW},
        ])
        await conn.execute(insert(hl_positions), [
            {"address": A, "dex": "", "coin": "BTC", "szi": Decimal("2"), "entry_px": 90000,
             "liquidation_px": 70000, "leverage": 3, "notional_usd": 200000,
             "unrealized_pnl": 20000, "updated_at": NOW},
            {"address": B, "dex": "", "coin": "BTC", "szi": Decimal("-1"), "entry_px": 110000,
             "liquidation_px": 130000, "leverage": 4, "notional_usd": 100000,
             "unrealized_pnl": -10000, "updated_at": NOW},
            {"address": C, "dex": "xyz", "coin": "BTC", "szi": Decimal("10"), "entry_px": 80000,
             "liquidation_px": 60000, "leverage": 2, "notional_usd": 1000000,
             "unrealized_pnl": 100000, "updated_at": NOW},
            {"address": D, "dex": "", "coin": "ETH", "szi": Decimal("20"), "entry_px": 3000,
             "liquidation_px": 2000, "leverage": 2, "notional_usd": 80000,
             "unrealized_pnl": 1000, "updated_at": NOW},
        ])


async def test_whale_rows_combines_filters_sorting_and_limit(engine):
    await _seed(engine)
    rows = await _query("whale_rows")(
        engine,
        account_value_min=Decimal("1000000"),
        pnl_30d_min=Decimal("0"),
        win_rate_min=Decimal("0.7"),
        style="directional",
        tier="focus",
        has_position=True,
        sort_by="pnl_30d",
        sort_order="desc",
        limit=200,
    )
    assert [row["address"] for row in rows] == [A]
    assert rows[0]["short_address"] == "0x1111…1111"
    assert rows[0]["display_name"] == "方向鲸"
    assert rows[0]["total_notional"] == "200000"

    ordered = await _query("whale_rows")(engine, sort_by="account_value", sort_order="desc", limit=2)
    assert [row["address"] for row in ordered] == [C, D]
    assert len(ordered) == 2


async def test_net_positions_only_counts_fresh_focus_and_core_with_current_mids(engine):
    await _seed(engine)
    # B 的空仓改成陈旧快照，候选 C 与排除 D 无论多大都不能计入。
    async with engine.begin() as conn:
        await conn.execute(
            hl_positions.update().where(hl_positions.c.address == B).values(updated_at=NOW - timedelta(minutes=20))
        )
    rows = await _query("whale_net_positions")(
        engine,
        mids={"BTC": Decimal("100000"), "ETH": Decimal("4000")},
        now=NOW,
        stale_after_s=600,
    )
    assert rows == [{
        "coin": "BTC",
        "long_usd": "200000",
        "short_usd": "0",
        "net_usd": "200000",
        "direction": "long",
        "updated_at": NOW.isoformat(),
    }]


async def test_whale_detail_exposes_kpis_curve_positions_and_recent_operations(engine):
    await _seed(engine)
    async with engine.begin() as conn:
        await conn.execute(insert(events).values(
            source="hl_monitor", category="hyperliquid", event_type="hl_increase", occurred_at=NOW,
            collected_at=NOW, title="巨鲸加仓 BTC", assets=["BTC"], final_level="A",
            dedupe_key="hl:detail", raw={"address": A, "coin": "BTC"},
        ))
        await conn.execute(insert(hl_fills).values(
            address=A, tid=7, coin="BTC", dir="Open Long", side="B", px=100000, sz=1,
            closed_pnl=0, fee=1, crossed=True, occurred_at=NOW - timedelta(minutes=1), hash="0xabc",
        ))

    detail = await _query("whale_detail")(engine, A.upper().replace("0X", "0x"), mids={"BTC": Decimal("100000")})
    assert detail is not None
    assert detail["wallet"]["address"] == A
    assert detail["profile"]["pnl_30d"] == "500000"
    assert detail["coverage"]["pnl_30d"]["complete"] is True
    assert detail["pnl_history"] == [[1000, "0"], [2000, "500000"]]
    assert detail["positions"][0]["direction"] == "long"
    assert detail["positions"][0]["liquidation_distance"] == "0.3"
    assert {item["kind"] for item in detail["operations"]} == {"event", "fill"}
    assert await _query("whale_detail")(engine, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", mids={}) is None


def test_market_tickers_preserve_config_order_and_statuses():
    rows = _query("market_tickers")(
        ["BTC", "ETH", "SOL"],
        {"BTC": Decimal("100000"), "ETH": Decimal("4000")},
        enabled=True,
        connected=False,
    )
    assert [row["symbol"] for row in rows] == ["BTC", "ETH", "SOL"]
    assert rows[0]["status"] == "断线" and rows[0]["price"] == "100000"
    assert rows[2]["status"] == "缺价" and rows[2]["price"] is None
    disabled = _query("market_tickers")(["BTC"], {}, enabled=False, connected=False)
    assert disabled == [{"symbol": "BTC", "price": None, "status": "未启用"}]
