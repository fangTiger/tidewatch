from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
import respx
from sqlalchemy import insert, select

from fakes import async_timeout
from tidewatch.collectors.base import TokenBucket
from tidewatch.hyperliquid.client import REST_URL, HyperliquidRestClient
from tidewatch.hyperliquid.tiers import TierRecalculator
from tidewatch.store.tables import hl_positions, hl_state, hl_wallets


def _address(number: int) -> str:
    return f"0x{number:040x}"


async def _insert_wallets(engine, rows):
    defaults = {"note": None, "excluded": False, "style": None, "profile": {}}
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets), [{**defaults, **row} for row in rows])


async def _tiers(engine) -> dict[str, str | None]:
    async with engine.connect() as conn:
        rows = (await conn.execute(select(hl_wallets.c.address, hl_wallets.c.tier))).all()
    return dict(rows)


@respx.mock
@async_timeout()
async def test_recompute_queries_candidates_with_batch_client_and_replaces_lowest(engine, clock):
    core, manual, automatic, eligible, below, maker, excluded = [_address(i) for i in range(1, 8)]
    await _insert_wallets(
        engine,
        [
            {"address": core, "tier": "core", "source": "manual", "profile": {"manual_core": True}},
            {"address": manual, "tier": "focus", "source": "manual", "profile": {"manual_focus": True}},
            {"address": automatic, "tier": "focus", "source": "leaderboard"},
            {"address": eligible, "tier": "candidate", "source": "leaderboard"},
            {"address": below, "tier": "candidate", "source": "leaderboard"},
            {"address": maker, "tier": "candidate", "source": "leaderboard", "style": "market_maker"},
            {"address": excluded, "tier": "candidate", "source": "leaderboard", "excluded": True},
        ],
    )
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_positions).values(
                address=automatic,
                dex="",
                coin="BTC",
                szi=1,
                notional_usd=Decimal("1100000"),
                updated_at=clock.now,
            )
        )

    requested: list[str] = []

    def response(request: httpx.Request) -> httpx.Response:
        payload = __import__("json").loads(request.content)
        requested.append(payload["user"])
        value = "2000000" if payload["user"] == eligible else "999999"
        return httpx.Response(200, json={"marginSummary": {"totalNtlPos": value}, "assetPositions": []})

    route = respx.post(REST_URL).mock(side_effect=response)
    client = HyperliquidRestClient(
        TokenBucket(1000, capacity=1000),
        TokenBucket(1000, capacity=1000),
        proxy_mode="direct",
    )
    try:
        result = await asyncio.wait_for(
            TierRecalculator(engine, client, focus_limit=2, clock=clock).recompute(),
            timeout=2,
        )
    finally:
        await client.aclose()

    assert route.call_count == 2
    assert set(requested) == {eligible, below}
    assert result.queried == 2 and result.promoted == 1 and result.demoted == 1
    tiers = await _tiers(engine)
    assert tiers[core] == "core" and tiers[manual] == "focus"
    assert tiers[eligible] == "focus" and tiers[automatic] == "candidate"
    assert tiers[below] == tiers[maker] == "candidate"
    assert tiers[excluded] is None


@async_timeout()
async def test_seven_day_zero_position_demotion_and_manual_core_protection(engine, clock):
    stale, recent, manual, core = [_address(i) for i in range(20, 24)]
    await _insert_wallets(
        engine,
        [
            {
                "address": stale,
                "tier": "focus",
                "source": "leaderboard",
                "last_position_at": clock.now - timedelta(days=7),
            },
            {
                "address": recent,
                "tier": "focus",
                "source": "leaderboard",
                "last_position_at": clock.now - timedelta(days=6, hours=23),
            },
            {
                "address": manual,
                "tier": "focus",
                "source": "manual",
                "last_position_at": clock.now - timedelta(days=30),
            },
            {
                "address": core,
                "tier": "core",
                "source": "manual",
                "last_position_at": clock.now - timedelta(days=30),
            },
        ],
    )

    class NoRequests:
        async def info(self, *args, **kwargs):
            raise AssertionError("没有候选时不应请求 REST")

    result = await TierRecalculator(engine, NoRequests(), focus_limit=3, clock=clock).recompute()
    tiers = await _tiers(engine)
    assert tiers == {stale: "candidate", recent: "focus", manual: "focus", core: "core"}
    assert result.demoted == 1


@async_timeout()
async def test_non_promotable_style_demotes_automatic_focus(engine, clock):
    maker = _address(24)
    await _insert_wallets(
        engine,
        [
            {
                "address": maker,
                "tier": "focus",
                "source": "leaderboard",
                "style": "market_maker",
                "last_position_at": clock.now,
            }
        ],
    )

    class NoRequests:
        async def info(self, *args, **kwargs):
            raise AssertionError("非晋升风格不应请求 REST")

    result = await TierRecalculator(engine, NoRequests(), clock=clock).recompute()
    assert await _tiers(engine) == {maker: "candidate"}
    assert result.demoted == 1


@async_timeout()
async def test_recompute_reloads_wallet_state_after_network_queries(engine, clock):
    address = _address(25)
    await _insert_wallets(
        engine,
        [{"address": address, "tier": "candidate", "source": "leaderboard"}],
    )

    class ExcludingRest:
        async def info(self, *args, **kwargs):
            async with engine.begin() as conn:
                await conn.execute(
                    hl_wallets.update()
                    .where(hl_wallets.c.address == address)
                    .values(tier=None, excluded=True)
                )
            return {"marginSummary": {"totalNtlPos": "2000000"}}

    result = await TierRecalculator(engine, ExcludingRest(), clock=clock).recompute()
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                select(hl_wallets.c.tier, hl_wallets.c.excluded).where(hl_wallets.c.address == address)
            )
        ).one()
    assert row.tier is None and row.excluded is True
    assert result.promoted == 0


@async_timeout()
async def test_recompute_progress_cursor_resumes_after_failure(engine, clock):
    first, second = _address(30), _address(31)
    await _insert_wallets(
        engine,
        [
            {"address": first, "tier": "candidate", "source": "leaderboard"},
            {"address": second, "tier": "candidate", "source": "leaderboard"},
        ],
    )

    class InterruptingRest:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def info(self, request_type: str, **kwargs):
            self.calls.append(kwargs["user"])
            if len(self.calls) == 2:
                raise RuntimeError("中断")
            return {"marginSummary": {"totalNtlPos": "1500000"}}

    interrupted = InterruptingRest()
    calculator = TierRecalculator(engine, interrupted, clock=clock)
    with pytest.raises(RuntimeError, match="中断"):
        await calculator.recompute()

    async with engine.connect() as conn:
        state = (await conn.execute(select(hl_state).where(hl_state.c.name == "tier_recompute"))).one()
    assert state.cursor["phase"] == "querying" and state.cursor["next_index"] == 1

    class ResumedRest:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def info(self, request_type: str, **kwargs):
            self.calls.append(kwargs["user"])
            return {"marginSummary": {"totalNtlPos": "2000000"}}

    resumed = ResumedRest()
    result = await TierRecalculator(engine, resumed, clock=clock).recompute()
    assert resumed.calls == [second]
    assert result.queried == 2
    async with engine.connect() as conn:
        state = (await conn.execute(select(hl_state).where(hl_state.c.name == "tier_recompute"))).one()
    assert state.cursor["phase"] == "complete" and state.cursor["processed"] == 2
