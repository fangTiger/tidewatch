from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from sqlalchemy import delete, func, insert, select

from fakes import FakeHealth, async_timeout
from tidewatch.collectors.base import TokenBucket
from tidewatch.hyperliquid.client import REST_URL, HyperliquidRestClient
from tidewatch.hyperliquid.monitor import (
    HyperliquidMonitor,
    PriorityReconcileQueue,
    ReconcilePriority,
    _all_dex_states,
    combined_account_value,
    select_hot_wallets,
)
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.tables import (
    collector_state,
    events,
    hl_fills,
    hl_positions,
    hl_state,
    hl_wallets,
    push_log,
)


ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
OTHER = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def _state(
    *,
    coin: str = "BTC",
    szi: str = "60",
    value: str = "6000000",
    liquidation_px: str | None = "95",
) -> dict:
    return {
        "marginSummary": {"accountValue": "800000", "totalNtlPos": value},
        "assetPositions": [
            {
                "type": "oneWay",
                "position": {
                    "coin": coin,
                    "szi": szi,
                    "leverage": {"type": "cross", "value": 20},
                    "entryPx": "100000",
                    "positionValue": value,
                    "unrealizedPnl": "1000",
                    "liquidationPx": liquidation_px,
                },
            }
        ],
    }


def _empty_state() -> dict:
    return {
        "marginSummary": {"accountValue": "800000", "totalNtlPos": "0"},
        "assetPositions": [],
    }


def test_real_all_dexs_ws_fixture_parses_main_and_hip3_states():
    message = json.loads(
        (FIXTURES / "all_dexs_clearinghouse_state_ws.json").read_text(encoding="utf-8")
    )

    states = _all_dex_states(message["data"])

    assert [dex for dex, _ in states] == ["", "xyz"]
    assert states[0][1]["assetPositions"][0]["position"]["coin"] == "BTC"
    assert states[1][1]["assetPositions"][0]["position"]["coin"] == "xyz:GOLD"


async def _seed_baseline(engine, clock, address: str = ADDRESS) -> None:
    """为只关注后续行为的测试预置已完成的监控基线。"""
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_state).values(
                name=f"monitor_baseline:{address}",
                cursor={"established_at": clock.now.isoformat()},
                updated_at=clock.now,
            )
        )
        await conn.execute(
            insert(hl_state).values(
                name=f"fills:{address}",
                cursor={"last_time": int(clock.now.timestamp() * 1000)},
                updated_at=clock.now,
            )
        )


async def test_first_reconcile_with_existing_positions_only_establishes_baseline(
    engine, settings, clock
):
    calls: list[dict] = []

    class Rest:
        async def try_info(self, request_type: str, **params):
            calls.append({"type": request_type, **params})
            if request_type == "clearinghouseState":
                return True, _state(szi="120", value="12000000", liquidation_px="98000")
            if request_type == "userAbstraction":
                return True, "disabled"
            raise AssertionError(f"首次基线不应补查成交：{request_type}")

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    try:
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
        await asyncio.wait_for(monitor.update_mids({"BTC": "100000"}), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_count = await conn.scalar(select(func.count()).select_from(events))
        baseline = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"monitor_baseline:{ADDRESS}")
        )
        fill_cursor = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"fills:{ADDRESS}")
        )
        risk = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"liq_risk:{ADDRESS}::BTC"
            )
        )

    assert event_count == 0
    assert baseline == {
        "established_at": clock.now.isoformat(),
        "last_observed_at": clock.now.isoformat(),
    }
    assert fill_cursor["last_time"] == int(clock.now.timestamp() * 1000)
    assert risk == {"level": "S"}
    assert [call["type"] for call in calls] == ["clearinghouseState", "userAbstraction"]


async def test_web_callbacks_run_after_position_commit_and_market_update(engine, settings, clock):
    """M6 SSE 回调只能观察到已提交仓位，行情回调收到原子更新后的完整 mids。"""

    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "clearinghouseState":
                return True, _state(szi="2", value="200000")
            if request_type == "userAbstraction":
                return True, "disabled"
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    committed: list[tuple[str, Decimal]] = []
    markets: list[dict[str, Decimal]] = []

    async def on_position(address: str, mids) -> None:
        async with engine.connect() as conn:
            size = await conn.scalar(select(hl_positions.c.szi).where(hl_positions.c.address == address))
        committed.append((address, Decimal(size)))

    async def on_market(mids) -> None:
        markets.append(dict(mids))

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
        on_position=on_position,
        on_market=on_market,
    )
    assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=3)
    await asyncio.wait_for(monitor.update_mids({"BTC": "100000", "ETH": "4000"}), timeout=3)
    assert committed == [(ADDRESS, Decimal("2"))]
    assert markets == [{"BTC": Decimal("100000"), "ETH": Decimal("4000")}]


async def test_open_after_baseline_emits_one_event(engine, settings, clock):
    clearing_calls = 0

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal clearing_calls
            if request_type == "clearinghouseState":
                clearing_calls += 1
                return True, _empty_state() if clearing_calls == 1 else _state()
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type == "userFillsByTime":
                return True, []
            if request_type == "userTwapSliceFills":
                return True, []
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    try:
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
        clock.advance(seconds=1)
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_types = (await conn.execute(select(events.c.event_type))).scalars().all()
    assert event_types == ["hl_open"]


async def test_stale_snapshot_change_without_fill_is_backfill_and_not_pushed(
    engine, settings, clock
):
    clearing_calls = 0

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal clearing_calls
            if request_type == "clearinghouseState":
                clearing_calls += 1
                state = _empty_state() if clearing_calls == 1 else _state()
                return True, state
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type in {"userFillsByTime", "userTwapSliceFills"}:
                return True, []
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    try:
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
        baseline_at = clock.now
        clock.advance(minutes=30)
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        row = (await conn.execute(select(events.c.occurred_at, events.c.raw))).one()
        push_count = await conn.scalar(select(func.count()).select_from(push_log))
    assert row.occurred_at == baseline_at
    assert row.raw["backfill"] is True
    assert push_count == 0


async def test_backfill_path_does_not_persist_real_small_close(
    engine, settings, clock
):
    clearing_calls = 0

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal clearing_calls
            if request_type == "clearinghouseState":
                clearing_calls += 1
                state = (
                    _state(
                        coin="WLD",
                        szi="1",
                        value="116",
                        liquidation_px=None,
                    )
                    if clearing_calls == 1
                    else _empty_state()
                )
                return True, state
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type in {"userFillsByTime", "userTwapSliceFills"}:
                return True, []
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    try:
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
        clock.advance(minutes=30)
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_count = await conn.scalar(select(func.count()).select_from(events))
    assert event_count == 0


async def test_core_ws_path_does_not_persist_real_small_reverse(
    engine, settings, clock
):
    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "userAbstraction":
                return True, "disabled"
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual")
        )
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    first = {
        "channel": "allDexsClearinghouseState",
        "data": {
            "user": ADDRESS,
            "clearinghouseStates": {
                "": _state(
                    coin="UMA",
                    szi="1",
                    value="1397",
                    liquidation_px=None,
                )
            },
        },
    }
    second = {
        "channel": "allDexsClearinghouseState",
        "data": {
            "user": ADDRESS,
            "clearinghouseStates": {
                "": _state(
                    coin="UMA",
                    szi="-1",
                    value="2083",
                    liquidation_px=None,
                )
            },
        },
    }
    try:
        await asyncio.wait_for(monitor.handle_core_message(first), timeout=5)
        clock.advance(seconds=5)
        await asyncio.wait_for(monitor.handle_core_message(second), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_count = await conn.scalar(select(func.count()).select_from(events))
    assert event_count == 0


async def test_missing_fill_cursor_starts_at_baseline_and_ignores_historical_liquidation(
    engine, settings, clock
):
    clearing_calls = 0
    fill_requests: list[dict] = []
    historical_time = int((clock.now - timedelta(hours=1)).timestamp() * 1000)
    historical_fill = {
        "coin": "BTC",
        "px": "100000",
        "sz": "60",
        "dir": "Liquidated Long",
        "closedPnl": "-100",
        "fee": "1",
        "crossed": True,
        "time": historical_time,
        "hash": "0xold",
        "tid": 501,
        "liquidation": {"liquidatedUser": ADDRESS},
    }

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal clearing_calls
            if request_type == "clearinghouseState":
                clearing_calls += 1
                state = _state() if clearing_calls == 1 else _state(szi="120", value="12000000")
                return True, state
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type == "userFillsByTime":
                fill_requests.append(dict(params))
                return True, [historical_fill]
            if request_type == "userTwapSliceFills":
                return True, [{"fill": historical_fill, "twapId": 1}]
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    try:
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
        baseline_ms = int(clock.now.timestamp() * 1000)
        async with engine.begin() as conn:
            await conn.execute(delete(hl_state).where(hl_state.c.name == f"fills:{ADDRESS}"))
        clock.advance(seconds=1)
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_types = (await conn.execute(select(events.c.event_type))).scalars().all()
        fill_count = await conn.scalar(select(func.count()).select_from(hl_fills))
    assert fill_requests == [{"budget": "monitor", "user": ADDRESS, "startTime": baseline_ms}]
    assert event_types == ["hl_increase"]
    assert fill_count == 0


async def test_hyperliquid_events_do_not_create_hl_monitor_collector_state(
    engine, settings, clock
):
    clearing_calls = 0

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal clearing_calls
            if request_type == "clearinghouseState":
                clearing_calls += 1
                return True, _empty_state() if clearing_calls == 1 else _state()
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type in {"userFillsByTime", "userTwapSliceFills"}:
                return True, []
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    try:
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
        clock.advance(seconds=1)
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        names = (await conn.execute(select(collector_state.c.name))).scalars().all()
    assert "hl_monitor" not in names


def test_priority_queue_keeps_deferred_low_priority_and_deduplicates_wallet():
    queue = PriorityReconcileQueue()
    now = 100.0
    queue.schedule(ADDRESS, ReconcilePriority.NORMAL, due_at=now, reason="普通对账")
    queue.schedule(ADDRESS, ReconcilePriority.NORMAL, due_at=now + 30, reason="重复普通对账")
    task = queue.pop_ready(now)
    assert task is not None and task.reason == "普通对账"

    queue.defer(task, due_at=now + 1)
    queue.schedule(OTHER, ReconcilePriority.TRADE, due_at=now, reason="成交触发")
    assert queue.pop_ready(now).address == OTHER
    assert queue.pop_ready(now) is None
    assert queue.pop_ready(now + 1).address == ADDRESS
    assert len(queue) == 0


def test_trade_debounce_replaces_existing_periodic_task_without_duplicates():
    queue = PriorityReconcileQueue()
    queue.schedule(ADDRESS, ReconcilePriority.NORMAL, due_at=180, reason="普通对账")
    queue.schedule(ADDRESS, ReconcilePriority.TRADE, due_at=2, reason="成交触发")
    queue.schedule(ADDRESS, ReconcilePriority.TRADE, due_at=3, reason="成交触发")

    assert len(queue) == 1
    assert queue.pop_ready(2.9) is None
    task = queue.pop_ready(3)
    assert task is not None
    assert task.priority is ReconcilePriority.TRADE


def test_trade_debounce_is_capped_by_first_pending_deadline():
    queue = PriorityReconcileQueue()
    queue.schedule(
        ADDRESS,
        ReconcilePriority.TRADE,
        due_at=3,
        deadline_at=5,
        reason="成交触发",
    )
    for now in (1, 2, 3, 4):
        queue.schedule(
            ADDRESS,
            ReconcilePriority.TRADE,
            due_at=now + 3,
            deadline_at=now + 5,
            reason="成交触发",
        )

    assert queue.pop_ready(4.99) is None
    task = queue.pop_ready(5)
    assert task is not None and task.address == ADDRESS


def test_aged_normal_task_runs_under_continuous_trade_load():
    queue = PriorityReconcileQueue(normal_aging_s=120, hot_aging_s=30)
    queue.schedule(ADDRESS, ReconcilePriority.NORMAL, due_at=0, reason="普通对账")
    for index in range(121):
        trade = f"0x{index + 100:040x}"
        queue.schedule(trade, ReconcilePriority.TRADE, due_at=index, reason="成交触发")
        task = queue.pop_ready(float(index))
        assert task is not None
        if index < 120:
            assert task.address == trade
        else:
            assert task.address == ADDRESS


def test_deferral_keeps_newer_trade_task_for_same_wallet():
    queue = PriorityReconcileQueue()
    queue.schedule(ADDRESS, ReconcilePriority.NORMAL, due_at=0, reason="普通对账")
    normal = queue.pop_ready(0)
    assert normal is not None
    queue.schedule(
        ADDRESS,
        ReconcilePriority.TRADE,
        due_at=3,
        deadline_at=5,
        reason="成交触发",
    )

    queue.defer(normal, due_at=10)

    task = queue.pop_ready(3)
    assert task is not None
    assert task.priority is ReconcilePriority.TRADE


def test_deferred_normal_task_ages_from_original_due_time():
    queue = PriorityReconcileQueue(normal_aging_s=120)
    queue.schedule(ADDRESS, ReconcilePriority.NORMAL, due_at=0, reason="普通对账")
    normal = queue.pop_ready(0)
    assert normal is not None
    queue.defer(normal, due_at=120)
    queue.schedule(OTHER, ReconcilePriority.TRADE, due_at=120, reason="成交触发")

    task = queue.pop_ready(120)
    assert task is not None and task.address == ADDRESS


def test_hot_wallets_sort_by_liquidation_distance_then_notional_and_limit(settings):
    rows = []
    mids = {"BTC": Decimal("100")}
    for index in range(35):
        rows.append(
            {
                "address": f"0x{index + 1:040x}",
                "coin": "BTC",
                "liquidation_px": Decimal(str(90 + index / 10)),
                "notional_usd": Decimal(str(10_000_000 + index)),
            }
        )

    selected = select_hot_wallets(rows, mids, settings.hyperliquid)

    assert len(selected) == 30
    assert selected[0].address == f"0x{35:040x}"
    assert selected[-1].address == f"0x{6:040x}"


def test_unified_account_value_adds_spot_usdc_only_for_unified_accounts():
    clearing = {"marginSummary": {"accountValue": "800000"}}
    spot = {"balances": [{"coin": "USDC", "total": "300000"}, {"coin": "HYPE", "total": "9"}]}

    assert combined_account_value("unifiedAccount", clearing, spot) == Decimal("1100000")
    assert combined_account_value("portfolioMargin", clearing, spot) == Decimal("1100000")
    assert combined_account_value("disabled", clearing, spot) == Decimal("800000")


async def test_rest_reconcile_persists_snapshot_fill_and_cursor_then_ingests_event(
    engine, settings, clock
):
    now_ms = int(clock.now.timestamp() * 1000)
    requests: list[dict] = []
    clearing_calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal clearing_calls
        payload = json.loads(request.content)
        requests.append(payload)
        request_type = payload["type"]
        if request_type == "clearinghouseState":
            clearing_calls += 1
            return httpx.Response(200, json=_empty_state() if clearing_calls == 1 else _state())
        if request_type == "userAbstraction":
            return httpx.Response(200, json="unifiedAccount")
        if request_type == "spotClearinghouseState":
            return httpx.Response(
                200,
                json={"balances": [{"coin": "USDC", "total": "300000", "hold": "0"}]},
            )
        if request_type == "userFillsByTime":
            return httpx.Response(
                200,
                json=[
                    {
                        "coin": "BTC",
                        "px": "100000",
                        "sz": "60",
                        "dir": "Open Long",
                        "closedPnl": "0",
                        "fee": "1",
                        "crossed": True,
                        "time": now_ms,
                        "hash": "0xabc",
                        "tid": 42,
                    }
                ],
            )
        raise AssertionError(f"未预期请求：{payload}")

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    rest = HyperliquidRestClient(
        TokenBucket(1_000, capacity=1_000),
        TokenBucket(1_000, capacity=1_000),
        proxy_mode="direct",
        health=FakeHealth(),
    )
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=rest,
        sink=sink,
        clock=clock,
    )
    try:
        with respx.mock:
            respx.post(REST_URL).mock(side_effect=respond)
            assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
            assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)
        await asyncio.wait_for(rest.aclose(), timeout=5)

    assert [item["type"] for item in requests].count("userFillsByTime") == 1
    async with engine.connect() as conn:
        position = (await conn.execute(select(hl_positions))).mappings().one()
        fill = (await conn.execute(select(hl_fills))).mappings().one()
        event = (await conn.execute(select(events))).mappings().one()
        cursor = await conn.scalar(select(hl_state.c.cursor).where(hl_state.c.name == f"fills:{ADDRESS}"))
        pushes = await conn.scalar(select(func.count()).select_from(push_log))

    assert position["dex"] == "" and position["notional_usd"] == Decimal("6000000")
    assert fill["tid"] == 42 and fill["address"] == ADDRESS
    assert event["dedupe_key"].endswith(":hl_open:42")
    assert event["raw"]["actual_leverage"] == str(Decimal("6000000") / Decimal("1100000"))
    assert cursor["last_time"] == now_ms and cursor["last_query_at"] == clock.now.isoformat()
    assert pushes == 1


async def test_multi_request_reconcile_resumes_completed_steps_after_budget_deferral(
    engine, settings, clock
):
    class Rest:
        def __init__(self) -> None:
            self.allowance = 1
            self.calls: list[str] = []

        async def try_info(self, request_type: str, **params):
            self.calls.append(request_type)
            if not self.allowance:
                return False, None
            self.allowance -= 1
            if request_type == "clearinghouseState":
                return True, {"marginSummary": {"accountValue": "800000"}, "assetPositions": []}
            if request_type == "userAbstraction":
                return True, "disabled"
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    rest = Rest()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=rest,
        sink=None,
        clock=clock,
    )

    assert not await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    rest.allowance = 1
    assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    assert rest.calls == ["clearinghouseState", "userAbstraction", "userAbstraction"]


async def test_reconcile_starts_independent_state_and_account_queries_concurrently(
    engine, settings, clock
):
    started: set[str] = set()
    both_started = asyncio.Event()
    release = asyncio.Event()

    class Rest:
        async def try_info(self, request_type: str, **params):
            started.add(request_type)
            if started == {"clearinghouseState", "userAbstraction"}:
                both_started.set()
            await release.wait()
            if request_type == "clearinghouseState":
                return True, _empty_state()
            if request_type == "userAbstraction":
                return True, "disabled"
            raise AssertionError(request_type)

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
    )
    task = asyncio.create_task(monitor.reconcile(ADDRESS))
    try:
        await asyncio.wait_for(both_started.wait(), timeout=0.2)
        release.set()
        assert await asyncio.wait_for(task, timeout=1)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_account_type_cache_refreshes_after_one_hour(engine, settings, clock):
    abstraction_calls = 0

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal abstraction_calls
            if request_type == "clearinghouseState":
                return True, _empty_state()
            if request_type == "userAbstraction":
                abstraction_calls += 1
                return True, "disabled"
            raise AssertionError(request_type)

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
    )
    assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=1)
    clock.advance(seconds=settings.hyperliquid.frequencies.account_type_cache_s - 1)
    assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=1)
    clock.advance(seconds=1)
    assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=1)

    assert abstraction_calls == 2


async def test_unmatched_fill_lookup_falls_back_to_twap_and_minute_guard_does_not_delay_event(
    engine, settings, clock, caplog
):
    calls: list[str] = []

    class Rest:
        async def try_info(self, request_type: str, **params):
            calls.append(request_type)
            if request_type == "userFillsByTime":
                return True, [
                    {
                        "coin": "ETH",
                        "px": "2000",
                        "sz": "2",
                        "dir": "Open Long",
                        "closedPnl": "0",
                        "fee": "0.2",
                        "crossed": True,
                        "time": int(clock.now.timestamp() * 1000),
                        "hash": "0xeth",
                        "tid": 98,
                    }
                ]
            if request_type == "userTwapSliceFills":
                return True, [
                    {
                        "fill": {
                            "coin": "BTC",
                            "px": "100",
                            "sz": "1",
                            "dir": "Open Long",
                            "closedPnl": "0",
                            "fee": "0.1",
                            "crossed": True,
                            "time": int(clock.now.timestamp() * 1000),
                            "hash": "0x" + "0" * 64,
                            "tid": 99,
                        },
                        "twapId": 7,
                    }
                ]
            raise AssertionError(request_type)

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
    )
    await _seed_baseline(engine, clock)
    first = await asyncio.wait_for(monitor.lookup_fill_evidence(ADDRESS, {"BTC"}), timeout=5)
    async with engine.connect() as conn:
        first_cursor = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"fills:{ADDRESS}")
        )
    assert first_cursor["last_query_at"] == clock.now.isoformat()
    caplog.set_level("DEBUG", logger="tidewatch.hyperliquid.monitor")
    second = await asyncio.wait_for(monitor.lookup_fill_evidence(ADDRESS, {"BTC"}), timeout=5)

    assert first["tid"] == 99 and second is None
    assert any("成交补查节流中" in record.message for record in caplog.records)
    assert not any("成交补查超过" in record.message for record in caplog.records)
    assert calls == ["userFillsByTime", "userTwapSliceFills"]
    assert monitor.deferred_fill_count == 1
    clock.advance(seconds=settings.hyperliquid.frequencies.fill_query_min_interval_s)
    await asyncio.wait_for(monitor.run_due_fill_queries(), timeout=5)
    assert monitor.deferred_fill_count == 0
    assert calls == [
        "userFillsByTime",
        "userTwapSliceFills",
        "userFillsByTime",
        "userTwapSliceFills",
    ]
    async with engine.connect() as conn:
        rows = (await conn.execute(select(hl_fills).order_by(hl_fills.c.tid))).mappings().all()
    assert [(row["tid"], row["is_twap"]) for row in rows] == [(98, False), (99, True)]


async def test_successful_fill_query_starts_minute_guard_when_twap_budget_is_exhausted(
    engine, settings, clock
):
    calls: list[str] = []

    class Rest:
        async def try_info(self, request_type: str, **params):
            calls.append(request_type)
            if request_type == "userFillsByTime":
                return True, []
            if request_type == "userTwapSliceFills":
                return False, None
            raise AssertionError(request_type)

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
    )
    await _seed_baseline(engine, clock)

    assert await asyncio.wait_for(
        monitor.lookup_fill_evidence(ADDRESS, {"BTC"}), timeout=5
    ) is None
    clock.advance(seconds=1)
    assert await asyncio.wait_for(
        monitor.lookup_fill_evidence(ADDRESS, {"BTC"}), timeout=5
    ) is None

    assert calls == ["userFillsByTime", "userTwapSliceFills"]
    async with engine.connect() as conn:
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"fills:{ADDRESS}")
        )
    assert cursor["last_query_at"] == (clock.now - timedelta(seconds=1)).isoformat()


async def test_deferred_fill_query_is_restored_after_monitor_restart(engine, settings, clock):
    calls: list[str] = []

    class Rest:
        async def try_info(self, request_type: str, **params):
            calls.append(request_type)
            if request_type == "userFillsByTime":
                return True, []
            if request_type == "userTwapSliceFills":
                return False, None
            raise AssertionError(request_type)

    first = HyperliquidMonitor(
        settings.hyperliquid, engine=engine, rest=Rest(), sink=None, clock=clock
    )
    await _seed_baseline(engine, clock)
    await asyncio.wait_for(first.lookup_fill_evidence(ADDRESS, {"BTC"}), timeout=5)

    clock.advance(seconds=settings.hyperliquid.frequencies.fill_query_min_interval_s)
    restored = HyperliquidMonitor(
        settings.hyperliquid, engine=engine, rest=Rest(), sink=None, clock=clock
    )
    await asyncio.wait_for(restored.run_due_fill_queries(), timeout=5)

    assert calls == [
        "userFillsByTime",
        "userTwapSliceFills",
        "userFillsByTime",
        "userTwapSliceFills",
    ]


async def test_fill_lookup_timeout_does_not_hold_snapshot_event(
    engine, settings, clock, caplog
):
    clearing_calls = 0

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal clearing_calls
            if request_type == "clearinghouseState":
                clearing_calls += 1
                states = [
                    _empty_state(),
                    _state(szi="60", value="6000000"),
                    _state(szi="30", value="3000000"),
                    _state(szi="60", value="6000000"),
                ]
                return True, states[min(clearing_calls - 1, len(states) - 1)]
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type == "userFillsByTime":
                await asyncio.Event().wait()
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
        fill_lookup_timeout_s=0.01,
    )
    caplog.set_level("DEBUG", logger="tidewatch.hyperliquid.monitor")
    try:
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=0.5)
        clock.advance(seconds=1)
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=0.5)
        clock.advance(seconds=90)
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=0.5)
        clock.advance(seconds=settings.hyperliquid.frequencies.fill_timeout_warning_interval_s)
        assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=0.5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_keys = (await conn.execute(select(events.c.dedupe_key))).scalars().all()
        cursor = await conn.scalar(select(hl_state.c.cursor).where(hl_state.c.name == f"fills:{ADDRESS}"))
    assert len(event_keys) == 3
    assert all(key.startswith(f"hl:{ADDRESS}:BTC:") for key in event_keys)
    assert cursor["pending_coins"] == ["BTC"]
    timeout_records = [
        record for record in caplog.records if "成交补查超过" in record.message
    ]
    assert [record.levelname for record in timeout_records] == ["WARNING", "DEBUG", "WARNING"]


async def test_deferred_fill_timeout_is_wallet_rate_limited_and_identifies_real_timeout(
    engine, settings, clock, caplog
):
    settings.hyperliquid.frequencies.deferred_fill_lookup_timeout_s = 0.02

    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "userFillsByTime":
                await asyncio.Event().wait()
            raise AssertionError(request_type)

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
        fill_lookup_timeout_s=0.005,
    )
    await _seed_baseline(engine, clock)
    await asyncio.wait_for(
        monitor._defer_fill_query(ADDRESS, {"BTC"}, clock.now), timeout=5
    )
    caplog.set_level("DEBUG", logger="tidewatch.hyperliquid.monitor")

    await asyncio.wait_for(monitor.run_due_fill_queries(), timeout=1)
    clock.advance(seconds=1)
    await asyncio.wait_for(monitor.run_due_fill_queries(), timeout=1)
    clock.advance(
        seconds=settings.hyperliquid.frequencies.fill_timeout_warning_interval_s
    )
    await asyncio.wait_for(monitor.run_due_fill_queries(), timeout=1)

    records = [
        record
        for record in caplog.records
        if "延期成交补查超过" in record.message
    ]
    assert [record.levelname for record in records] == ["WARNING", "DEBUG", "WARNING"]
    assert all("真实网络超时" in record.message for record in records)
    assert all("0.02 秒" in record.message for record in records)


@async_timeout()
async def test_liquidation_risk_cleanup_keeps_only_monitored_current_positions(
    engine, settings, clock
):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {"address": ADDRESS, "tier": "focus", "source": "manual"},
                {"address": OTHER, "tier": "candidate", "source": "leaderboard"},
            ],
        )
        await conn.execute(
            insert(hl_positions),
            [
                {
                    "address": ADDRESS,
                    "dex": "",
                    "coin": "BTC",
                    "szi": Decimal("1"),
                    "notional_usd": Decimal("1000000"),
                    "updated_at": clock.now,
                },
                {
                    "address": OTHER,
                    "dex": "xyz",
                    "coin": "xyz:GOLD",
                    "szi": Decimal("1"),
                    "notional_usd": Decimal("1000000"),
                    "updated_at": clock.now,
                },
            ],
        )
        await conn.execute(
            insert(hl_state),
            [
                {
                    "name": f"liq_risk:{ADDRESS}::BTC",
                    "cursor": {"level": "A"},
                    "updated_at": clock.now,
                },
                {
                    "name": f"liq_risk:{ADDRESS}::ETH",
                    "cursor": {"level": "B"},
                    "updated_at": clock.now,
                },
                {
                    "name": f"liq_risk:{OTHER}:xyz:xyz:GOLD",
                    "cursor": {"level": "S"},
                    "updated_at": clock.now,
                },
            ],
        )
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=None,
        clock=clock,
    )

    deleted = await asyncio.wait_for(
        monitor.cleanup_liquidation_risk_states(), timeout=5
    )

    async with engine.connect() as conn:
        names = set(
            (
                await conn.execute(
                    select(hl_state.c.name).where(hl_state.c.name.like("liq_risk:%"))
                )
            ).scalars()
        )
    assert deleted == 2
    assert names == {f"liq_risk:{ADDRESS}::BTC"}


def test_normal_and_deferred_fill_timeouts_share_wallet_warning_limit(
    engine, settings, clock, caplog
):
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=None,
        clock=clock,
    )
    caplog.set_level("DEBUG", logger="tidewatch.hyperliquid.monitor")

    monitor._log_fill_timeout(ADDRESS)
    monitor._log_fill_timeout(ADDRESS, deferred=True)
    monitor._log_fill_timeout(OTHER, deferred=True)

    records = [
        record for record in caplog.records if "成交补查超过" in record.message
    ]
    assert [record.levelname for record in records] == ["WARNING", "DEBUG", "WARNING"]


async def test_deferred_fill_budget_deferral_is_debug_not_timeout(
    engine, settings, clock, caplog
):
    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "userFillsByTime":
                return False, None
            raise AssertionError(request_type)

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
    )
    await _seed_baseline(engine, clock)
    assert await asyncio.wait_for(
        monitor.lookup_fill_evidence(ADDRESS, {"BTC"}), timeout=5
    ) is None
    caplog.set_level("DEBUG", logger="tidewatch.hyperliquid.monitor")
    caplog.clear()
    clock.advance(seconds=1)

    await asyncio.wait_for(monitor.run_due_fill_queries(), timeout=5)

    records = [
        record
        for record in caplog.records
        if "延期成交补查" in record.message
    ]
    assert [(record.levelname, record.message) for record in records] == [
        (
            "DEBUG",
            f"钱包 {ADDRESS[:6]}…{ADDRESS[-4:]} 延期成交补查因 REST 预算不足，将继续顺延",
        )
    ]
    assert not any("超过" in record.message for record in records)


async def test_empty_account_lookup_errors_use_exception_type(
    engine, settings, clock, caplog
):
    class AccountRest:
        async def try_info(self, request_type: str, **params):
            if request_type == "userAbstraction":
                raise asyncio.TimeoutError()
            raise AssertionError(request_type)

    account_monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=AccountRest(),
        sink=None,
        clock=clock,
    )
    caplog.set_level("WARNING", logger="tidewatch.hyperliquid.monitor")
    account_value = await asyncio.wait_for(
        account_monitor._core_account_value(ADDRESS, _empty_state()),
        timeout=1,
    )

    assert account_value == Decimal("800000")
    assert any(
        "账户类型查询失败，暂按普通账户处理：TimeoutError()" in record.message
        for record in caplog.records
    )
    caplog.clear()

    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "userAbstraction":
                return True, "unifiedAccount"
            if request_type == "spotClearinghouseState":
                raise asyncio.TimeoutError()
            raise AssertionError(request_type)

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
    )
    value = await asyncio.wait_for(
        monitor._core_account_value(ADDRESS, _empty_state()),
        timeout=1,
    )

    assert value == Decimal("800000")
    assert any(
        "现货 USDC 查询失败，使用最近缓存：TimeoutError()" in record.message
        for record in caplog.records
    )


async def test_spot_lookup_timeout_uses_rest_compatible_timeout_and_wallet_warning_limit(
    engine, settings, clock, caplog
):
    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "userAbstraction":
                return True, "unifiedAccount"
            if request_type == "spotClearinghouseState":
                raise asyncio.TimeoutError()
            raise AssertionError(request_type)

    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=None,
        clock=clock,
    )
    assert monitor.account_lookup_timeout_s >= monitor.fill_lookup_timeout_s
    caplog.set_level("DEBUG", logger="tidewatch.hyperliquid.monitor")

    await asyncio.wait_for(monitor._core_account_value(ADDRESS, _empty_state()), timeout=1)
    await asyncio.wait_for(monitor._core_account_value(ADDRESS, _empty_state()), timeout=1)
    clock.advance(seconds=settings.hyperliquid.frequencies.fill_timeout_warning_interval_s)
    await asyncio.wait_for(monitor._core_account_value(ADDRESS, _empty_state()), timeout=1)

    records = [
        record
        for record in caplog.records
        if "现货 USDC 查询失败，使用最近缓存" in record.message
    ]
    assert [record.levelname for record in records] == ["WARNING", "DEBUG", "WARNING"]


async def test_reconcile_refreshes_known_hip3_dex_without_overwriting_main_dex(
    engine, settings, clock
):
    requests: list[dict] = []

    def state_for(dex: str) -> dict:
        if dex == "xyz":
            return {
                "marginSummary": {"accountValue": "1000000", "totalNtlPos": "600000"},
                "assetPositions": [
                    {
                        "type": "oneWay",
                        "position": {
                            "coin": "xyz:SILVER",
                            "szi": "2",
                            "leverage": {"type": "cross", "value": 5},
                            "entryPx": "300000",
                            "positionValue": "600000",
                            "unrealizedPnl": "0",
                            "liquidationPx": None,
                        },
                    }
                ],
            }
        return {"marginSummary": {"accountValue": "1000000", "totalNtlPos": "0"}, "assetPositions": []}

    def respond(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        requests.append(payload)
        if payload["type"] == "clearinghouseState":
            return httpx.Response(200, json=state_for(payload.get("dex", "")))
        if payload["type"] == "userAbstraction":
            return httpx.Response(200, json="disabled")
        if payload["type"] in {"userFillsByTime", "userTwapSliceFills"}:
            return httpx.Response(200, json=[])
        raise AssertionError(payload)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
        await conn.execute(
            insert(hl_positions).values(
                address=ADDRESS,
                dex="xyz",
                coin="xyz:SILVER",
                szi=Decimal("1"),
                entry_px=Decimal("300000"),
                liquidation_px=None,
                leverage=Decimal("5"),
                notional_usd=Decimal("300000"),
                unrealized_pnl=Decimal("0"),
                updated_at=clock.now - timedelta(minutes=1),
            )
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    rest = HyperliquidRestClient(
        TokenBucket(1_000, capacity=1_000),
        TokenBucket(1_000, capacity=1_000),
        proxy_mode="direct",
    )
    monitor = HyperliquidMonitor(settings.hyperliquid, engine=engine, rest=rest, sink=sink, clock=clock)
    try:
        with respx.mock:
            respx.post(REST_URL).mock(side_effect=respond)
            assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)
        await asyncio.wait_for(rest.aclose(), timeout=5)

    clearing_requests = [item for item in requests if item["type"] == "clearinghouseState"]
    assert clearing_requests == [
        {"type": "clearinghouseState", "user": ADDRESS},
        {"type": "clearinghouseState", "user": ADDRESS, "dex": "xyz"},
    ]
    async with engine.connect() as conn:
        row = (await conn.execute(select(hl_positions))).mappings().one()
        event_type = await conn.scalar(select(events.c.event_type))
    assert row["dex"] == "xyz" and row["szi"] == Decimal("2")
    assert event_type == "hl_increase"


async def test_known_hip3_dex_remains_reconciled_after_position_closes(engine, settings, clock):
    requests: list[dict] = []

    class Rest:
        async def try_info(self, request_type: str, **params):
            requests.append({"type": request_type, **params})
            if request_type == "clearinghouseState":
                return True, {
                    "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0"},
                    "assetPositions": [],
                }
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type in {"userFillsByTime", "userTwapSliceFills"}:
                return True, []
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
        await conn.execute(
            insert(hl_positions).values(
                address=ADDRESS,
                dex="xyz",
                coin="xyz:SILVER",
                szi=Decimal("1"),
                entry_px=Decimal("300000"),
                liquidation_px=None,
                leverage=Decimal("5"),
                notional_usd=Decimal("300000"),
                unrealized_pnl=Decimal("0"),
                updated_at=clock.now,
            )
        )
        await conn.execute(
            insert(hl_state).values(
                name=f"liq_risk:{ADDRESS}:xyz:xyz:SILVER",
                cursor={"level": "A"},
                updated_at=clock.now,
            )
        )
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(settings.hyperliquid, engine=engine, rest=Rest(), sink=sink, clock=clock)
    try:
        await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
        requests.clear()
        clock.advance(seconds=61)
        await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    clearing = [item for item in requests if item["type"] == "clearinghouseState"]
    assert clearing == [
        {"type": "clearinghouseState", "budget": "monitor", "user": ADDRESS},
        {"type": "clearinghouseState", "budget": "monitor", "user": ADDRESS, "dex": "xyz"},
    ]
    async with engine.connect() as conn:
        risk = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"liq_risk:{ADDRESS}:xyz:xyz:SILVER"
            )
        )
    assert risk is None


async def test_configured_hip3_dex_is_queried_before_wallet_has_local_position(
    engine, settings, clock
):
    requests: list[dict] = []

    class Rest:
        async def try_info(self, request_type: str, **params):
            requests.append({"type": request_type, **params})
            if request_type == "clearinghouseState":
                return True, {
                    "marginSummary": {"accountValue": "1000000", "totalNtlPos": "0"},
                    "assetPositions": [],
                }
            if request_type == "userAbstraction":
                return True, "disabled"
            raise AssertionError(request_type)

    settings.hyperliquid.hip3_dexes = ["xyz"]
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    monitor = HyperliquidMonitor(
        settings.hyperliquid, engine=engine, rest=Rest(), sink=None, clock=clock
    )

    assert await asyncio.wait_for(monitor.reconcile(ADDRESS), timeout=5)
    clearing = [item for item in requests if item["type"] == "clearinghouseState"]
    assert clearing == [
        {"type": "clearinghouseState", "budget": "monitor", "user": ADDRESS},
        {
            "type": "clearinghouseState",
            "budget": "monitor",
            "user": ADDRESS,
            "dex": "xyz",
        },
    ]


async def test_all_mids_uses_local_prices_for_hysteresis_and_a_to_s_upgrade(
    engine, settings, clock
):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
        await conn.execute(
            insert(hl_positions).values(
                address=ADDRESS,
                dex="",
                coin="BTC",
                szi=Decimal("120"),
                entry_px=Decimal("100"),
                liquidation_px=Decimal("98"),
                leverage=Decimal("20"),
                notional_usd=Decimal("12000000"),
                unrealized_pnl=Decimal("0"),
                updated_at=clock.now,
            )
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=sink,
        clock=clock,
    )
    try:
        await asyncio.wait_for(monitor.update_mids({"BTC": "103"}), timeout=5)
        async with engine.connect() as conn:
            first_push_id = await conn.scalar(select(push_log.c.id))
        await asyncio.wait_for(sink.push_result(first_push_id, "dry_run"), timeout=5)
        clock.advance(seconds=1)
        await asyncio.wait_for(monitor.update_mids({"BTC": "102.9"}), timeout=5)
        clock.advance(seconds=1)
        await asyncio.wait_for(monitor.update_mids({"BTC": "100"}), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        rows = (await conn.execute(select(events.c.raw).order_by(events.c.id))).scalars().all()
        pushes = (await conn.execute(select(push_log.c.level, push_log.c.kind).order_by(push_log.c.id))).all()
    assert [row["liquidation_risk_level"] for row in rows] == ["A", "S"]
    assert pushes == [("A", "event"), ("S", "upgrade")]


async def test_all_mids_risk_retry_after_post_ingest_crash_is_exactly_once(
    engine, settings, clock
):
    class CrashAfterIngest:
        def __init__(self, inner) -> None:
            self.inner = inner
            self.crashed = False

        async def ingest(self, *args, **kwargs):
            result = await self.inner.ingest(*args, **kwargs)
            if not self.crashed:
                self.crashed = True
                raise RuntimeError("故障注入：风险事件已提交但滞回状态尚未写入")
            return result

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
        await conn.execute(
            insert(hl_positions).values(
                address=ADDRESS,
                dex="",
                coin="BTC",
                szi=Decimal("120"),
                entry_px=Decimal("100"),
                liquidation_px=Decimal("95"),
                leverage=Decimal("20"),
                notional_usd=Decimal("12000000"),
                unrealized_pnl=Decimal("0"),
                updated_at=clock.now,
            )
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    crashing = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=CrashAfterIngest(sink),
        clock=clock,
    )
    try:
        with pytest.raises(RuntimeError, match="故障注入"):
            await asyncio.wait_for(crashing.update_mids({"BTC": "100"}), timeout=5)
        clock.advance(seconds=1)
        restarted = HyperliquidMonitor(
            settings.hyperliquid,
            engine=engine,
            rest=None,
            sink=sink,
            clock=clock,
        )
        await asyncio.wait_for(restarted.update_mids({"BTC": "100"}), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        keys = (await conn.execute(select(events.c.dedupe_key))).scalars().all()
        push_count = await conn.scalar(select(func.count()).select_from(push_log))
    assert len(keys) == 1
    assert keys[0].endswith(":hl_liq_risk:1790733600000:A")
    assert push_count == 1


async def test_user_fills_snapshot_is_persisted_but_not_used_as_event_evidence(
    engine, settings, clock
):
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=sink,
        clock=clock,
    )
    fill = {
        "coin": "BTC",
        "px": "100000",
        "sz": "60",
        "dir": "Open Long",
        "closedPnl": "0",
        "fee": "1",
        "crossed": True,
        "time": int(clock.now.timestamp() * 1000),
        "hash": "0xabc",
        "tid": 77,
    }
    try:
        await asyncio.wait_for(
            monitor.handle_core_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [_empty_state()]},
                }
            ),
            timeout=5,
        )
        await asyncio.wait_for(
            monitor.handle_core_message(
                {"channel": "userFills", "data": {"user": ADDRESS, "isSnapshot": True, "fills": [fill]}}
            ),
            timeout=5,
        )
        await asyncio.wait_for(
            monitor.handle_core_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [["", _state()]]},
                }
            ),
            timeout=5,
        )
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        fill_count = await conn.scalar(select(func.count()).select_from(hl_fills))
        event = (await conn.execute(select(events.c.dedupe_key))).scalar_one()
    assert fill_count == 1
    assert not event.endswith(":77")


async def test_live_user_fill_becomes_state_change_evidence(engine, settings, clock):
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=sink,
        clock=clock,
    )
    fill = {
        "coin": "BTC",
        "px": "100000",
        "sz": "60",
        "dir": "Open Long",
        "closedPnl": "0",
        "fee": "1",
        "crossed": True,
        "time": int(clock.now.timestamp() * 1000),
        "hash": "0xlive",
        "tid": 88,
    }
    try:
        await asyncio.wait_for(
            monitor.handle_core_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [_empty_state()]},
                }
            ),
            timeout=5,
        )
        await asyncio.wait_for(
            monitor.handle_core_message(
                {"channel": "userFills", "data": {"user": ADDRESS, "isSnapshot": False, "fills": [fill]}}
            ),
            timeout=5,
        )
        await asyncio.wait_for(
            monitor.handle_core_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [_state()]},
                }
            ),
            timeout=5,
        )
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        dedupe_key = await conn.scalar(select(events.c.dedupe_key))
    assert dedupe_key.endswith(":88")


async def test_core_ws_unified_account_uses_current_spot_usdc(engine, settings, clock):
    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "userAbstraction":
                return True, "unifiedAccount"
            if request_type == "spotClearinghouseState":
                return True, {"balances": [{"coin": "USDC", "total": "300000"}]}
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=sink,
        clock=clock,
    )
    try:
        await asyncio.wait_for(
            monitor.handle_core_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [_empty_state()]},
                }
            ),
            timeout=5,
        )
        await asyncio.wait_for(
            monitor.handle_core_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [_state()]},
                }
            ),
            timeout=5,
        )
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        raw = await conn.scalar(select(events.c.raw))
    assert raw["actual_leverage"] == str(Decimal("6000000") / Decimal("1100000"))


async def test_snapshot_event_retry_after_post_ingest_crash_is_exactly_once(
    engine, settings, clock
):
    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "clearinghouseState":
                return True, _state()
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type in {"userFillsByTime", "userTwapSliceFills"}:
                return True, []
            raise AssertionError(request_type)

    class CrashAfterIngest:
        def __init__(self, inner) -> None:
            self.inner = inner
            self.crashed = False

        async def ingest(self, *args, **kwargs):
            result = await self.inner.ingest(*args, **kwargs)
            if not self.crashed:
                self.crashed = True
                raise RuntimeError("故障注入：事件已提交但快照尚未写入")
            return result

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
        await conn.execute(
            insert(hl_positions).values(
                address=ADDRESS,
                dex="",
                coin="BTC",
                szi=Decimal("0"),
                notional_usd=Decimal("0"),
                updated_at=clock.now,
            )
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    crashing = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=CrashAfterIngest(sink),
        clock=clock,
    )
    try:
        with pytest.raises(RuntimeError, match="故障注入"):
            await asyncio.wait_for(crashing.reconcile(ADDRESS), timeout=5)
        clock.advance(seconds=1)
        restarted = HyperliquidMonitor(
            settings.hyperliquid,
            engine=engine,
            rest=Rest(),
            sink=sink,
            clock=clock,
        )
        assert await asyncio.wait_for(restarted.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_count = await conn.scalar(select(func.count()).select_from(events))
        push_count = await conn.scalar(select(func.count()).select_from(push_log))
    assert event_count == 1
    assert push_count == 1


async def test_fill_backed_snapshot_retry_recovers_tid_from_persisted_fill(
    engine, settings, clock
):
    now_ms = int(clock.now.timestamp() * 1000)
    fill = {
        "coin": "BTC",
        "px": "100000",
        "sz": "60",
        "dir": "Open Long",
        "closedPnl": "0",
        "fee": "1",
        "crossed": True,
        "time": now_ms,
        "hash": "0xfill-retry",
        "tid": 709,
    }

    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "clearinghouseState":
                return True, _state()
            if request_type == "userAbstraction":
                return True, "disabled"
            if request_type == "userFillsByTime":
                return True, [fill]
            raise AssertionError(request_type)

    class CrashAfterIngest:
        def __init__(self, inner) -> None:
            self.inner = inner

        async def ingest(self, *args, **kwargs):
            await self.inner.ingest(*args, **kwargs)
            raise RuntimeError("故障注入：tid 事件已提交但快照尚未写入")

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
        await conn.execute(
            insert(hl_positions).values(
                address=ADDRESS,
                dex="",
                coin="BTC",
                szi=Decimal("0"),
                notional_usd=Decimal("0"),
                updated_at=clock.now,
            )
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    crashing = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=Rest(),
        sink=CrashAfterIngest(sink),
        clock=clock,
    )
    try:
        with pytest.raises(RuntimeError, match="故障注入"):
            await asyncio.wait_for(crashing.reconcile(ADDRESS), timeout=5)
        clock.advance(seconds=1)
        restarted = HyperliquidMonitor(
            settings.hyperliquid,
            engine=engine,
            rest=Rest(),
            sink=sink,
            clock=clock,
        )
        assert await asyncio.wait_for(restarted.reconcile(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        keys = (await conn.execute(select(events.c.dedupe_key))).scalars().all()
        push_count = await conn.scalar(select(func.count()).select_from(push_log))
    assert keys == [f"hl:{ADDRESS}:BTC:hl_open:709"]
    assert push_count == 1


def _liquidation_fill(*, tid: int = 700, liquidated_user: str = ADDRESS) -> dict:
    return {
        "coin": "BTC",
        "px": "100000",
        "sz": "60",
        "startPosition": "60",
        "side": "A",
        "dir": "Liquidated Long",
        "closedPnl": "-500000",
        "fee": "1",
        "crossed": True,
        "time": 1791252001000,
        "hash": f"0x{tid:064x}",
        "tid": tid,
        "liquidation": {"liquidatedUser": liquidated_user, "method": "market"},
    }


async def test_state_before_liquidation_fill_still_emits_from_persisted_fill(
    engine, settings, clock
):
    class Rest:
        async def try_info(self, request_type: str, **params):
            if request_type == "userAbstraction":
                return True, "disabled"
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual")
        )
        await conn.execute(
            insert(hl_positions).values(
                address=ADDRESS,
                dex="",
                coin="BTC",
                szi=Decimal("60"),
                entry_px=Decimal("100000"),
                notional_usd=Decimal("6000000"),
                updated_at=clock.now,
            )
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid, engine=engine, rest=Rest(), sink=sink, clock=clock
    )
    try:
        await asyncio.wait_for(
            monitor.handle_core_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [_empty_state()]},
                }
            ),
            timeout=5,
        )
        await asyncio.wait_for(
            monitor.handle_core_message(
                {
                    "channel": "userFills",
                    "data": {
                        "user": ADDRESS,
                        "isSnapshot": False,
                        "fills": [_liquidation_fill()],
                    },
                }
            ),
            timeout=5,
        )
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                select(events.c.event_type, events.c.amount_usd).order_by(events.c.id)
            )
        ).all()
    assert rows == [("hl_close", Decimal("6000000")), ("hl_liquidated", Decimal("6000000"))]


async def test_deferred_fill_lookup_emits_late_liquidation(engine, settings, clock):
    calls = 0

    class Rest:
        async def try_info(self, request_type: str, **params):
            nonlocal calls
            if request_type == "userFillsByTime":
                calls += 1
                return (False, None) if calls == 1 else (True, [_liquidation_fill(tid=701)])
            raise AssertionError(request_type)

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid, engine=engine, rest=Rest(), sink=sink, clock=clock
    )
    try:
        assert await asyncio.wait_for(
            monitor.lookup_fill_evidence(ADDRESS, {"BTC"}), timeout=5
        ) is None
        clock.advance(seconds=1)
        await asyncio.wait_for(monitor.run_due_fill_queries(), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_type = await conn.scalar(select(events.c.event_type))
    assert event_type == "hl_liquidated"


async def test_liquidation_cursor_recovers_after_event_commit_crash(
    engine, settings, clock
):
    class CrashAfterIngest:
        def __init__(self, inner) -> None:
            self.inner = inner

        async def ingest(self, *args, **kwargs):
            await self.inner.ingest(*args, **kwargs)
            raise RuntimeError("故障注入：清算事件已提交但游标尚未推进")

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    crashing = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=CrashAfterIngest(sink),
        clock=clock,
    )
    try:
        with pytest.raises(RuntimeError, match="故障注入"):
            await asyncio.wait_for(
                crashing._store_fills(ADDRESS, [(_liquidation_fill(tid=702), False)]),
                timeout=5,
            )
        restarted = HyperliquidMonitor(
            settings.hyperliquid,
            engine=engine,
            rest=None,
            sink=sink,
            clock=clock,
        )
        await asyncio.wait_for(restarted.process_liquidation_fills(ADDRESS), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_count = await conn.scalar(select(func.count()).select_from(events))
        push_count = await conn.scalar(select(func.count()).select_from(push_log))
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"liquidations:{ADDRESS}"
            )
        )
    assert event_count == 1
    assert push_count == 1
    assert cursor["last_tid"] == 702


async def test_restart_emits_fill_committed_before_liquidation_generation(
    engine, settings, clock
):
    class CrashBeforeLiquidation(HyperliquidMonitor):
        async def process_liquidation_fills(self, address: str) -> None:
            raise RuntimeError("故障注入：成交已提交但清算事件尚未生成")

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    crashing = CrashBeforeLiquidation(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=sink,
        clock=clock,
    )
    try:
        with pytest.raises(RuntimeError, match="故障注入"):
            await asyncio.wait_for(
                crashing._store_fills(ADDRESS, [(_liquidation_fill(tid=708), False)]),
                timeout=5,
            )
        async with engine.connect() as conn:
            assert await conn.scalar(select(func.count()).select_from(hl_fills)) == 1
            assert await conn.scalar(select(func.count()).select_from(events)) == 0

        restarted = HyperliquidMonitor(
            settings.hyperliquid,
            engine=engine,
            rest=None,
            sink=sink,
            clock=clock,
        )
        await asyncio.wait_for(restarted.process_pending_liquidations(), timeout=5)
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_count = await conn.scalar(select(func.count()).select_from(events))
        push_count = await conn.scalar(select(func.count()).select_from(push_log))
    assert event_count == 1
    assert push_count == 1


async def test_persisted_counterparty_liquidation_does_not_emit(
    engine, settings, clock
):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid, engine=engine, rest=None, sink=sink, clock=clock
    )
    try:
        await asyncio.wait_for(
            monitor._store_fills(
                ADDRESS,
                [(_liquidation_fill(tid=703, liquidated_user=OTHER), False)],
            ),
            timeout=5,
        )
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_count = await conn.scalar(select(func.count()).select_from(events))
    assert event_count == 0


async def test_late_older_liquidation_is_not_skipped_by_high_water_cursor(
    engine, settings, clock
):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    monitor = HyperliquidMonitor(
        settings.hyperliquid, engine=engine, rest=None, sink=sink, clock=clock
    )
    newer = {
        **_liquidation_fill(tid=704, liquidated_user=OTHER),
        "time": 1791252002000,
    }
    older_liquidation = {
        **_liquidation_fill(tid=705),
        "time": 1791252001000,
    }
    try:
        await asyncio.wait_for(
            monitor._store_fills(ADDRESS, [(newer, False)]), timeout=5
        )
        await asyncio.wait_for(
            monitor._store_fills(ADDRESS, [(older_liquidation, False)]), timeout=5
        )
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        tids = (await conn.execute(select(events.c.raw["tid"].as_integer()))).scalars().all()
    assert tids == [705]


async def test_processed_late_liquidation_is_not_replayed_on_later_fill(
    engine, settings, clock
):
    class CountingSink:
        def __init__(self, inner) -> None:
            self.inner = inner
            self.calls = 0

        async def ingest(self, *args, **kwargs):
            self.calls += 1
            return await self.inner.ingest(*args, **kwargs)

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual")
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    counting = CountingSink(sink)
    monitor = HyperliquidMonitor(
        settings.hyperliquid, engine=engine, rest=None, sink=counting, clock=clock
    )
    try:
        await asyncio.wait_for(
            monitor._store_fills(ADDRESS, [(_liquidation_fill(tid=710), False)]),
            timeout=5,
        )
        ordinary_fill = {
            **_liquidation_fill(tid=711, liquidated_user=OTHER),
            "time": 1791252002000,
            "liquidation": None,
        }
        await asyncio.wait_for(
            monitor._store_fills(ADDRESS, [(ordinary_fill, False)]), timeout=5
        )
    finally:
        await asyncio.wait_for(sink.stop(), timeout=5)

    assert counting.calls == 1


async def test_concurrent_liquidation_consumers_do_not_regress_cursor(
    engine, settings, clock
):
    class FirstBlockingSink:
        def __init__(self, inner) -> None:
            self.inner = inner
            self.calls = 0
            self.first_started = asyncio.Event()
            self.release_first = asyncio.Event()
            self.second_finished = asyncio.Event()

        async def ingest(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                self.first_started.set()
                await self.release_first.wait()
            result = await self.inner.ingest(*args, **kwargs)
            if self.calls == 2:
                self.second_finished.set()
            return result

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual")
        )
    await _seed_baseline(engine, clock)
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    blocking = FirstBlockingSink(sink)
    monitor = HyperliquidMonitor(
        settings.hyperliquid,
        engine=engine,
        rest=None,
        sink=blocking,
        clock=clock,
    )
    first = asyncio.create_task(
        monitor._store_fills(ADDRESS, [(_liquidation_fill(tid=706), False)])
    )
    second: asyncio.Task | None = None
    try:
        await asyncio.wait_for(blocking.first_started.wait(), timeout=1)
        second_fill = {
            **_liquidation_fill(tid=707),
            "time": 1791252002000,
        }
        second = asyncio.create_task(
            monitor._store_fills(ADDRESS, [(second_fill, False)])
        )
        try:
            await asyncio.wait_for(blocking.second_finished.wait(), timeout=0.1)
        except asyncio.TimeoutError:
            pass
        blocking.release_first.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=5)
    finally:
        blocking.release_first.set()
        pending = [task for task in (first, second) if task is not None and not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        cursor = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"liquidations:{ADDRESS}"
            )
        )
        tids = (
            await conn.execute(
                select(events.c.raw["tid"].as_integer()).order_by(events.c.id)
            )
        ).scalars().all()
    assert cursor["last_tid"] == 707
    assert tids == [706, 707]
