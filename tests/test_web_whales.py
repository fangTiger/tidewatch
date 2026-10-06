"""M6 巨鲸页面、名单操作与同源保护。"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote

import httpx
from sqlalchemy import insert, select

from fakes import wait_until
from tidewatch.store.tables import collector_state, hl_positions, hl_wallets
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.server import create_app


A = "0x1111111111111111111111111111111111111111"
B = "0x2222222222222222222222222222222222222222"


async def _seed(engine, clock) -> None:
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets), [
            {"address": A, "note": "<script>alert(1)</script>", "tier": "focus", "source": "manual",
             "excluded": False, "style": "directional",
             "profile": {"account_value": "2000000", "pnl_30d": "120000", "win_rate": 0.6,
                         "tags": ["<img src=x onerror=alert(2)>", "大户"],
                         "pnl_history": [[1, "0"], [2, "120000"]], "coverage": {}},
             "profile_updated_at": clock.now},
            {"address": B, "note": "第二只", "tier": "candidate", "source": "leaderboard",
             "excluded": False, "style": "swing",
             "profile": {"account_value": "1000000", "pnl_30d": "-1", "win_rate": 0.4},
             "profile_updated_at": clock.now},
        ])
        await conn.execute(insert(hl_positions).values(
            address=A, dex="", coin="BTC", szi=2, entry_px=90000, liquidation_px=70000,
            leverage=3, notional_usd=200000, unrealized_pnl=20000, updated_at=clock.now,
        ))


def _runtime() -> SimpleNamespace:
    return SimpleNamespace(monitor=SimpleNamespace(mids={"BTC": Decimal("100000"), "ETH": Decimal("4000")}))


async def _client(engine, settings, clock, *, enabled: bool = True, broadcaster: Broadcaster | None = None):
    app = create_app(
        engine,
        broadcaster or Broadcaster(),
        settings,
        clock=clock,
        hyperliquid=_runtime() if enabled else None,
    )
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_whales_list_filters_sorts_caps_and_escapes_external_text(engine, settings, clock):
    await _seed(engine, clock)
    async with await _client(engine, settings, clock) as http:
        page = await asyncio.wait_for(http.get("/whales", params={"tier": "focus", "sort": "pnl_30d"}), timeout=3)
        data = await asyncio.wait_for(http.get("/api/whales", params={"has_position": "1"}), timeout=3)
    assert page.status_code == 200
    assert "巨鲸名单" in page.text and A in page.text and B not in page.text
    assert "<script>alert(1)</script>" not in page.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page.text
    assert "<img src=x onerror=alert(2)>" not in page.text
    assert "&lt;img src=x onerror=alert(2)&gt;" in page.text
    assert data.status_code == 200 and [row["address"] for row in data.json()] == [A]
    assert 'href="/whales/' + A + '"' in page.text


async def test_whale_detail_and_unknown_404(engine, settings, clock):
    await _seed(engine, clock)
    async with await _client(engine, settings, clock) as http:
        page = await asyncio.wait_for(http.get(f"/whales/{A}"), timeout=3)
        missing = await asyncio.wait_for(http.get("/whales/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), timeout=3)
    assert page.status_code == 200
    for marker in (
        "钱包画像", "盈亏曲线", "当前仓位", "距强平", "最近操作", "30 天成交量",
        "最大回撤率", "未闭合持仓轮次", "偏好币种", "配置杠杆",
    ):
        assert marker in page.text
    assert "<svg" in page.text and 'id="positions"' in page.text
    assert missing.status_code == 404


async def test_whale_actions_require_same_origin_and_broadcast_after_commit(engine, settings, clock):
    broadcaster = Broadcaster()
    async with await _client(engine, settings, clock, broadcaster=broadcaster) as http:
        rejected = await asyncio.wait_for(
            http.post("/whales/actions", data={"action": "add", "address": A}), timeout=3
        )
        foreign = await asyncio.wait_for(
            http.post("/whales/actions", data={"action": "add", "address": A}, headers={"Origin": "https://evil.example"}),
            timeout=3,
        )
        agen = broadcaster.subscribe()
        pending = asyncio.create_task(asyncio.wait_for(agen.__anext__(), timeout=3))
        await wait_until(lambda: broadcaster.subscriber_count == 1, timeout=2)
        accepted = await asyncio.wait_for(
            http.post("/whales/actions", data={"action": "add", "address": A}, headers={"Origin": "http://test"}),
            timeout=3,
        )
        message = await pending
        await agen.aclose()
    assert rejected.status_code == 403 and foreign.status_code == 403
    assert accepted.status_code == 303 and "操作成功" in unquote(accepted.headers["location"])
    assert message["event"] == "wallet.updated" and json.loads(message["data"])["address"] == A
    async with engine.connect() as conn:
        row = (await conn.execute(select(hl_wallets).where(hl_wallets.c.address == A))).mappings().one()
    assert row["tier"] == "focus" and row["profile"]["manual_focus"] is True


async def test_whale_actions_share_registry_limits_conflicts_and_note_validation(engine, settings, clock):
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets), [
            {"address": f"0x{i:040x}", "tier": "core", "source": "manual", "excluded": False,
             "profile": {"manual_core": True}}
            for i in range(1, 11)
        ])
        await conn.execute(insert(hl_wallets).values(
            address=A, tier="focus", source="manual", excluded=False, profile={"manual_focus": True}
        ))
    async with await _client(engine, settings, clock) as http:
        limited = await asyncio.wait_for(
            http.post("/whales/actions", data={"action": "core", "address": A},
                      headers={"Referer": "http://test/whales"}, follow_redirects=True), timeout=3
        )
        conflict = await asyncio.wait_for(
            http.post("/whales/actions", data={"action": "exclude", "address": "0x0000000000000000000000000000000000000001"},
                      headers={"Origin": "http://test"}, follow_redirects=True), timeout=3
        )
        invalid = await asyncio.wait_for(
            http.post("/whales/actions", data={"action": "note", "address": "bad", "note": "备注"},
                      headers={"Origin": "http://test"}, follow_redirects=True), timeout=3
        )
    assert "核心钱包最多 10 个" in limited.text
    assert "核心钱包不能直接排除" in conflict.text
    assert "钱包地址必须是" in invalid.text


async def test_whale_actions_toggle_core_exclusion_and_note(engine, settings, clock):
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(
            address=A, tier="focus", source="manual", excluded=False, profile={"manual_focus": True}
        ))
    headers = {"Origin": "http://test"}
    async with await _client(engine, settings, clock) as http:
        for action, extra in (
            ("note", {"note": "我的 <script>备注</script>"}),
            ("core", {}),
            ("un_core", {}),
            ("exclude", {}),
            ("un_exclude", {}),
        ):
            response = await asyncio.wait_for(
                http.post("/whales/actions", data={"action": action, "address": A, **extra}, headers=headers),
                timeout=3,
            )
            assert response.status_code == 303
    async with engine.connect() as conn:
        row = (await conn.execute(select(hl_wallets).where(hl_wallets.c.address == A))).mappings().one()
    assert row["tier"] == "candidate" and row["excluded"] is False
    assert row["note"] == "我的 <script>备注</script>"


async def test_home_market_and_net_sidebar_have_enabled_and_disabled_states(engine, settings, clock):
    await _seed(engine, clock)
    async with await _client(engine, settings, clock, enabled=True) as http:
        page = await asyncio.wait_for(http.get("/"), timeout=3)
        summary = await asyncio.wait_for(http.get("/api/hyperliquid"), timeout=3)
    assert page.status_code == 200
    assert 'id="market-strip"' in page.text and "BTC" in page.text and "100000" in page.text
    assert 'id="net-positions"' in page.text and "200000" in page.text
    assert summary.json()["enabled"] is True

    async with engine.begin() as conn:
        await conn.execute(insert(collector_state).values(
            name="hl_core_ws", status="abnormal", mode="normal", consecutive_failures=2,
            last_error="价格连接断开",
        ))
    async with await _client(engine, settings, clock, enabled=True) as http:
        disconnected = await asyncio.wait_for(http.get("/api/hyperliquid"), timeout=3)
    assert next(item for item in disconnected.json()["tickers"] if item["symbol"] == "BTC")["status"] == "断线"

    async with await _client(engine, settings, clock, enabled=False) as http:
        disabled = await asyncio.wait_for(http.get("/"), timeout=3)
        disabled_api = await asyncio.wait_for(http.get("/api/hyperliquid"), timeout=3)
    assert "未启用" in disabled.text
    assert disabled_api.json() == {"enabled": False, "tickers": [
        {"symbol": symbol, "price": None, "status": "未启用"}
        for symbol in settings.hyperliquid.ticker_symbols
    ], "net_positions": [], "net_updated_at": None}


async def test_status_lists_five_hyperliquid_sources_independently(engine, settings, clock):
    rows = []
    for index, name in enumerate(("hl_rest", "hl_trades_ws", "hl_core_ws", "hl_leaderboard", "hl_profile")):
        rows.append({
            "name": name,
            "status": "abnormal" if name == "hl_leaderboard" else "normal",
            "mode": "normal",
            "last_success_at": clock.now - timedelta(hours=index),
            "last_event_at": clock.now - timedelta(minutes=index),
            "last_error": "导入失败" if name == "hl_leaderboard" else None,
            "consecutive_failures": 3 if name == "hl_leaderboard" else 0,
        })
    async with engine.begin() as conn:
        await conn.execute(insert(collector_state), rows)
    async with await _client(engine, settings, clock) as http:
        page = await asyncio.wait_for(http.get("/status"), timeout=3)
    assert page.status_code == 200
    assert all(f'id="s-{name}"' in page.text for name in ("hl_rest", "hl_trades_ws", "hl_core_ws", "hl_leaderboard", "hl_profile"))
    assert 'id="s-hl_leaderboard" class="st-abnormal"' in page.text
    assert 'id="s-hl_profile" class="st-normal"' in page.text
    assert "导入失败" in page.text


def test_m6_layout_has_dashboard_detail_and_narrow_screen_styles():
    css = (Path(__file__).resolve().parents[1] / "tidewatch/web/static/style.css").read_text(encoding="utf-8")
    for selector in (".dashboard-layout", ".market-strip", ".net-panel", ".kpi-grid", ".whale-grid"):
        assert selector in css
    assert "@media (max-width: 800px)" in css
