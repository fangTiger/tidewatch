from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal

import httpx
import respx
from sqlalchemy import func, insert, select
from websockets.asyncio.server import serve

from fakes import async_timeout
from tidewatch.hyperliquid.client import BASE_WEIGHTS
from tidewatch.hyperliquid.monitor import ReconcilePriority
from tidewatch.hyperliquid.runtime import HyperliquidRuntime
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.tables import events, hl_positions, hl_state, hl_wallets, push_log


ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
OTHER = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"


class Health:
    def __init__(self) -> None:
        self.successes: list[str] = []
        self.failures: list[tuple[str, str]] = []
        self.modes: list[tuple[str, str]] = []

    def register(self, name: str, stale_after: float) -> None:
        return None

    async def success(self, name: str) -> None:
        self.successes.append(name)

    async def failure(self, name: str, error: str) -> None:
        self.failures.append((name, error))

    async def set_mode(self, name: str, mode: str) -> None:
        self.modes.append((name, mode))


def _state(szi: str, value: str) -> dict:
    positions = []
    if Decimal(szi):
        positions.append(
            {
                "type": "oneWay",
                "position": {
                    "coin": "BTC",
                    "szi": szi,
                    "leverage": {"type": "cross", "value": 20},
                    "entryPx": "100000",
                    "positionValue": value,
                    "unrealizedPnl": "0",
                    "liquidationPx": "90000",
                },
            }
        )
    return {
        "marginSummary": {"accountValue": "2000000", "totalNtlPos": value},
        "assetPositions": positions,
    }


async def _wait_for_push(engine, timeout: float = 5) -> None:
    async def poll() -> None:
        while True:
            async with engine.connect() as conn:
                count = await conn.scalar(select(func.count()).select_from(push_log))
            if count:
                return
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout=timeout)


@async_timeout()
async def test_stop_cancels_inflight_rest_work_without_waiting_for_request_timeout(
    engine, settings, clock
):
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def inflight_rest(request_type: str, **params):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    runtime.rest.try_info = inflight_rest
    task = asyncio.create_task(runtime.run_rest())
    runtime._tasks = [task]
    await asyncio.wait_for(started.wait(), timeout=1)
    try:
        before = time.monotonic()
        await asyncio.wait_for(runtime.stop(), timeout=0.5)
        elapsed = time.monotonic() - before
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if not runtime._resources_closed:
            await runtime.close_resources()

    assert elapsed < 0.5
    assert task.cancelled() and cancelled.is_set()
    assert runtime.ws._closed.is_set()
    assert runtime.rest._client.is_closed and runtime.http.is_closed


@async_timeout(10)
async def test_core_ws_position_change_reaches_push_log_under_ten_seconds(
    engine, settings, clock
):
    subscriptions: list[dict] = []

    async def handler(websocket) -> None:
        while {item["subscription"]["type"] for item in subscriptions} != {
            "allMids",
            "userFills",
            "allDexsClearinghouseState",
        }:
            subscriptions.append(json.loads(await asyncio.wait_for(websocket.recv(), timeout=5)))
        await websocket.send(
            json.dumps(
                {
                    "channel": "userFills",
                    "data": {"user": ADDRESS, "isSnapshot": True, "fills": []},
                }
            )
        )
        await websocket.send(
            json.dumps(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [_state("0", "0")]},
                }
            )
        )
        await websocket.send(
            json.dumps(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [_state("60", "6000000")]},
                }
            )
        )
        await websocket.wait_closed()

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    async with serve(handler, "127.0.0.1", 0) as server:
        settings.hyperliquid.ws_url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        settings.hyperliquid.proxy = "direct"
        runtime = HyperliquidRuntime(
            settings.hyperliquid,
            Health(),
            engine=engine,
            sink=sink,
            clock=clock,
        )
        started = time.monotonic()
        tasks = [
            asyncio.create_task(runtime.run_core_ws()),
            asyncio.create_task(runtime.run_core_subscriptions()),
            asyncio.create_task(runtime.run_core_messages()),
            asyncio.create_task(runtime.run_market_data()),
        ]
        try:
            with respx.mock:
                respx.post(settings.hyperliquid.rest_url).mock(
                    return_value=httpx.Response(200, json="disabled")
                )
                await _wait_for_push(engine)
                assert time.monotonic() - started < 10
        finally:
            await runtime.stop()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_type = await conn.scalar(select(events.c.event_type))
        position = (await conn.execute(select(hl_positions))).mappings().one()
    assert event_type == "hl_open"
    assert position["szi"] == Decimal("60")


@async_timeout(10)
async def test_core_ws_reconnect_restores_subscriptions_and_keeps_final_position(
    engine, settings, clock
):
    connection_count = 0

    async def handler(websocket) -> None:
        nonlocal connection_count
        connection_count += 1
        received_types: set[str] = set()
        while received_types != {"allMids", "userFills", "allDexsClearinghouseState"}:
            message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))
            received_types.add(message["subscription"]["type"])
        state = _state("0", "0") if connection_count == 1 else _state("60", "6000000")
        await websocket.send(
            json.dumps(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": ADDRESS, "clearinghouseStates": [["", state]]},
                }
            )
        )
        if connection_count == 1:
            await websocket.close()
        else:
            await websocket.wait_closed()

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    async with serve(handler, "127.0.0.1", 0) as server:
        settings.hyperliquid.ws_url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        settings.hyperliquid.proxy = "direct"
        runtime = HyperliquidRuntime(
            settings.hyperliquid,
            Health(),
            engine=engine,
            sink=sink,
            clock=clock,
        )
        heartbeat_blocker = asyncio.Event()

        async def fast_reconnect(seconds: float) -> None:
            if seconds == settings.hyperliquid.frequencies.ws_ping_interval_s:
                await heartbeat_blocker.wait()
            else:
                await asyncio.sleep(0)

        runtime.ws._sleep = fast_reconnect
        tasks = [
            asyncio.create_task(runtime.run_core_ws()),
            asyncio.create_task(runtime.run_core_subscriptions()),
            asyncio.create_task(runtime.run_core_messages()),
            asyncio.create_task(runtime.run_market_data()),
        ]
        try:
            with respx.mock:
                respx.post(settings.hyperliquid.rest_url).mock(
                    return_value=httpx.Response(200, json="disabled")
                )
                await _wait_for_push(engine)
        finally:
            await runtime.stop()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            await asyncio.wait_for(sink.stop(), timeout=5)

    assert connection_count >= 2
    async with engine.connect() as conn:
        position = (await conn.execute(select(hl_positions))).mappings().one()
    assert position["szi"] == Decimal("60")


@async_timeout()
async def test_reconnect_hook_reconciles_hot_wallet_final_state_through_rest(
    engine, settings, clock
):
    def respond(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if payload["type"] == "clearinghouseState":
            return httpx.Response(200, json=_state("120", "14400000"))
        if payload["type"] == "userAbstraction":
            return httpx.Response(200, json="disabled")
        if payload["type"] in {"userFillsByTime", "userTwapSliceFills"}:
            return httpx.Response(200, json=[])
        raise AssertionError(payload)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=OTHER, tier="focus", source="manual"))
        await conn.execute(
            insert(hl_positions).values(
                address=OTHER,
                dex="",
                coin="BTC",
                szi=Decimal("100"),
                entry_px=Decimal("100000"),
                liquidation_px=Decimal("90000"),
                leverage=Decimal("20"),
                notional_usd=Decimal("12000000"),
                unrealized_pnl=Decimal("0"),
                updated_at=clock.now,
            )
        )
        await conn.execute(
            insert(hl_state),
            [
                {
                    "name": f"monitor_baseline:{OTHER}",
                    "cursor": {"established_at": clock.now.isoformat()},
                    "updated_at": clock.now,
                },
                {
                    "name": f"fills:{OTHER}",
                    "cursor": {"last_time": int(clock.now.timestamp() * 1000)},
                    "updated_at": clock.now,
                },
            ],
        )
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    now = {"value": 100.0}
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=sink,
        clock=clock,
        monotonic=lambda: now["value"],
    )
    try:
        with respx.mock:
            respx.post(settings.hyperliquid.rest_url).mock(side_effect=respond)
            await runtime._on_core_connected()
            now["value"] = 100.4
            assert await runtime.run_reconcile_once(now["value"])
    finally:
        await runtime.close_resources()
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        position = (await conn.execute(select(hl_positions))).mappings().one()
        event_type = await conn.scalar(select(events.c.event_type))
    assert position["szi"] == Decimal("120")
    assert event_type == "hl_increase"


@async_timeout()
async def test_focus_scheduler_uses_three_minute_period_and_budget_deferral_keeps_task(
    engine, settings, clock
):
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
        monotonic=lambda: 100.0,
    )
    outcomes = iter([False, True, True])
    calls: list[str] = []

    async def reconcile(address: str) -> bool:
        calls.append(address)
        return next(outcomes)

    runtime.monitor.reconcile = reconcile
    try:
        await runtime.schedule_focus_wallets(100.0)
        assert len(runtime.reconcile_queue) == 1
        await runtime.run_reconcile_once(100.0)
        assert len(runtime.reconcile_queue) == 1
        await runtime.schedule_focus_wallets(100.0)
        assert runtime.reconcile_queue.pop_ready(100.0) is None

        runtime.reconcile_queue.schedule(
            OTHER,
            ReconcilePriority.TRADE,
            due_at=100.0,
            reason="成交触发",
        )
        await runtime.run_reconcile_once(100.0)
        assert calls == [ADDRESS, OTHER]
        assert len(runtime.reconcile_queue) == 1

        await runtime.run_reconcile_once(100.1)
        assert calls == [ADDRESS, OTHER, ADDRESS]
        await runtime.schedule_focus_wallets(279.9)
        assert len(runtime.reconcile_queue) == 0
        await runtime.schedule_focus_wallets(280.1)
        assert len(runtime.reconcile_queue) == 1
    finally:
        await runtime.close_resources()


@async_timeout(5)
async def test_two_hundred_focus_wallets_finish_within_three_minute_latency_model(
    engine, settings, clock
):
    """按 160 倍缩放模拟每请求 0.8 秒，证明调度吞吐而非预算是关键。"""
    addresses = [f"0x{index + 1:040x}" for index in range(200)]
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {"address": address, "tier": "focus", "source": "manual"}
                for address in addresses
            ],
        )
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    runtime.monitor.hot_wallets = lambda: asyncio.sleep(0, result=[])
    request_delay_s = 0.005
    scale = 0.8 / request_delay_s
    active_wallets = 0
    max_active_wallets = 0
    completed = 0
    request_count = 0

    async def fake_request() -> None:
        nonlocal request_count
        request_count += 1
        await asyncio.sleep(request_delay_s)

    async def reconcile(address: str) -> bool:
        nonlocal active_wallets, max_active_wallets, completed
        active_wallets += 1
        max_active_wallets = max(max_active_wallets, active_wallets)
        try:
            await fake_request()
            await fake_request()
        finally:
            active_wallets -= 1
        completed += 1
        if completed == len(addresses):
            runtime._stop.set()
        return True

    runtime.monitor.reconcile = reconcile
    started_at = time.monotonic()
    try:
        await asyncio.wait_for(runtime.run_rest(), timeout=4)
    finally:
        await runtime.close_resources()
    modeled_elapsed_s = (time.monotonic() - started_at) * scale

    assert completed == 200
    assert request_count == 400
    assert 1 < max_active_wallets <= settings.hyperliquid.reconcile_concurrency
    assert modeled_elapsed_s <= settings.hyperliquid.frequencies.focus_reconcile_interval_s


@async_timeout()
async def test_reconcile_concurrency_keeps_same_wallet_single_flight(
    engine, settings, clock
):
    settings.hyperliquid.frequencies.trade_debounce_s = 0.01
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()
    calls = 0

    async def reconcile(address: str) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_started.set()
            await release_first.wait()
        else:
            second_started.set()
        return True

    runtime.monitor.reconcile = reconcile
    runtime.reconcile_queue.schedule(
        ADDRESS, ReconcilePriority.NORMAL, due_at=100.0, reason="普通对账"
    )
    runner = asyncio.create_task(runtime.run_reconcile_workers())
    try:
        await asyncio.wait_for(first_started.wait(), timeout=1)
        runtime.trigger_trade(ADDRESS)
        await asyncio.sleep(0.05)
        assert calls == 1
        release_first.set()
        await asyncio.wait_for(second_started.wait(), timeout=1)
    finally:
        runtime._stop.set()
        release_first.set()
        await asyncio.wait_for(runner, timeout=1)
        await runtime.close_resources()

    assert calls == 2


@async_timeout()
async def test_free_reconcile_slot_takes_trade_before_waiting_normal_wallets(
    engine, settings, clock
):
    settings.hyperliquid.reconcile_concurrency = 2
    settings.hyperliquid.frequencies.trade_debounce_s = 0.01
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    runtime.monitor.hot_wallets = lambda: asyncio.sleep(0, result=[])
    normal_addresses = [f"0x{index + 1:040x}" for index in range(4)]
    trade_address = "0x0000000000000000000000000000000000000999"
    releases = {address: asyncio.Event() for address in normal_addresses}
    first_wave_started = asyncio.Event()
    trade_started = asyncio.Event()
    started: list[str] = []

    async def reconcile(address: str) -> bool:
        started.append(address)
        if len(started) == 2:
            first_wave_started.set()
        if address == trade_address:
            trade_started.set()
            return True
        await releases[address].wait()
        return True

    runtime.monitor.reconcile = reconcile
    for address in normal_addresses:
        runtime.reconcile_queue.schedule(
            address,
            ReconcilePriority.NORMAL,
            due_at=0,
            reason="普通对账",
        )
    runner = asyncio.create_task(runtime.run_reconcile_workers())
    try:
        await asyncio.wait_for(first_wave_started.wait(), timeout=1)
        runtime.trigger_trade(trade_address)
        await asyncio.sleep(0.02)
        releases[started[0]].set()
        await asyncio.wait_for(trade_started.wait(), timeout=1)
    finally:
        runtime._stop.set()
        for release in releases.values():
            release.set()
        await asyncio.wait_for(runner, timeout=1)
        await runtime.close_resources()

    assert started[2] == trade_address


def test_two_hundred_wallet_budget_estimate_stays_below_six_hundred_per_minute(settings):
    """按最保守的全统一账户估算常态状态请求，不含按变化触发的成交补查。"""
    focus_count = settings.hyperliquid.tiers.focus_limit
    hot_count = settings.hyperliquid.tiers.hot_limit
    normal_count = focus_count - hot_count
    clearing_weight = BASE_WEIGHTS["clearinghouseState"]
    spot_weight = BASE_WEIGHTS["spotClearinghouseState"]
    abstraction_weight = BASE_WEIGHTS.get("userAbstraction", 2)
    normal_per_minute = (
        normal_count
        * (clearing_weight + spot_weight)
        / (settings.hyperliquid.frequencies.focus_reconcile_interval_s / 60)
    )
    hot_per_minute = (
        hot_count
        * (clearing_weight + spot_weight)
        / (settings.hyperliquid.frequencies.hot_reconcile_interval_s / 60)
    )
    abstraction_per_minute = (
        focus_count
        * abstraction_weight
        / (settings.hyperliquid.frequencies.account_type_cache_s / 60)
    )

    total_per_minute = normal_per_minute + hot_per_minute + abstraction_per_minute

    assert total_per_minute < settings.hyperliquid.monitor_budget_per_min
    assert settings.hyperliquid.frequencies.focus_reconcile_interval_s <= 180
    assert 593 < total_per_minute < 594


@async_timeout()
async def test_runtime_starts_with_empty_configured_ten_second_buckets(
    engine, settings, clock
):
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
        monotonic=lambda: 100.0,
    )
    try:
        assert runtime.monitoring_bucket.capacity == 100
        assert abs(runtime.batch_bucket.capacity - (200 / 6)) < 1e-9
        assert not await runtime.monitoring_bucket.try_acquire(1)
        assert not await runtime.batch_bucket.try_acquire(1)
    finally:
        await runtime.close_resources()


@async_timeout()
async def test_429_defers_reconcile_until_global_cooldown_without_wallet_backoff(
    engine, settings, clock
):
    now = {"value": 0.0}
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
        monotonic=lambda: now["value"],
    )
    responses = iter([httpx.Response(429, json={"error": "限流"}), httpx.Response(200, json={})])

    async def reconcile(address: str) -> bool:
        available, _ = await runtime.rest.try_info("clearinghouseState", user=address)
        return available

    runtime.monitor.reconcile = reconcile
    runtime.reconcile_queue.schedule(
        ADDRESS, ReconcilePriority.TRADE, due_at=0.0, reason="成交触发"
    )
    try:
        with respx.mock:
            route = respx.post(settings.hyperliquid.rest_url).mock(side_effect=responses)
            now["value"] = 0.2
            assert await runtime.run_reconcile_once(now["value"])
            assert route.call_count == 1
            assert ADDRESS in runtime.reconcile_queue
            assert ADDRESS not in runtime._reconcile_backoffs
            deferred = runtime.reconcile_queue._tasks[ADDRESS]
            assert deferred.priority is ReconcilePriority.TRADE
            assert deferred.reason == "成交触发"
            assert runtime.reconcile_queue.pop_ready(1.19) is None

            now["value"] = 1.2
            assert await runtime.run_reconcile_once(now["value"])
            assert route.call_count == 2
    finally:
        await runtime.close_resources()

    assert ADDRESS not in runtime.reconcile_queue


@async_timeout()
async def test_reconcile_transient_failures_back_off_without_blocking_queue(
    engine, settings, clock
):
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
    )

    async def fail(address: str) -> bool:
        raise ConnectionError("上游暂不可用")

    runtime.monitor.reconcile = fail
    runtime.reconcile_queue.schedule(
        ADDRESS, ReconcilePriority.NORMAL, due_at=100.0, reason="普通对账"
    )
    try:
        assert await runtime.run_reconcile_once(100.0)
        assert runtime.reconcile_queue.pop_ready(100.9) is None
        assert await runtime.run_reconcile_once(101.0)
        assert runtime.reconcile_queue.pop_ready(102.9) is None
        assert runtime.reconcile_queue.pop_ready(103.0) is not None
    finally:
        await runtime.close_resources()


@async_timeout()
async def test_focus_position_change_is_detected_on_three_minute_reconcile_cycle(
    engine, settings, clock
):
    clearing_calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal clearing_calls
        payload = json.loads(request.content)
        if payload["type"] == "clearinghouseState":
            clearing_calls += 1
            state = _state("0", "0") if clearing_calls == 1 else _state("60", "6000000")
            return httpx.Response(200, json=state)
        if payload["type"] == "userAbstraction":
            return httpx.Response(200, json="disabled")
        if payload["type"] in {"userFillsByTime", "userTwapSliceFills"}:
            return httpx.Response(200, json=[])
        raise AssertionError(payload)

    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="focus", source="manual"))
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    now = {"value": 100.0}
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=sink,
        clock=clock,
        monotonic=lambda: now["value"],
    )
    try:
        with respx.mock:
            respx.post(settings.hyperliquid.rest_url).mock(side_effect=respond)
            await runtime.schedule_focus_wallets(now["value"])
            now["value"] = 100.4
            assert await runtime.run_reconcile_once(now["value"])
            first_completed_at = now["value"]
            now["value"] = (
                first_completed_at
                + settings.hyperliquid.frequencies.focus_reconcile_interval_s
                - 0.1
            )
            await runtime.schedule_focus_wallets(now["value"])
            assert len(runtime.reconcile_queue) == 0
            now["value"] = (
                first_completed_at
                + settings.hyperliquid.frequencies.focus_reconcile_interval_s
            )
            await runtime.schedule_focus_wallets(now["value"])
            assert await runtime.run_reconcile_once(now["value"])
    finally:
        await runtime.close_resources()
        await asyncio.wait_for(sink.stop(), timeout=5)

    async with engine.connect() as conn:
        event_type = await conn.scalar(select(events.c.event_type))
        push_count = await conn.scalar(select(func.count()).select_from(push_log))
    assert event_type == "hl_open"
    assert push_count == 1
    assert abs(
        now["value"]
        - first_completed_at
        - settings.hyperliquid.frequencies.focus_reconcile_interval_s
    ) < 1e-9


@async_timeout()
async def test_core_subscription_sync_adds_and_removes_cli_changes(engine, settings, clock):
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets).values(address=ADDRESS, tier="core", source="manual"))
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    try:
        await runtime.sync_core_subscriptions()
        assert runtime.core_addresses == {ADDRESS}
        assert runtime.ws.subscription_count == 3
        async with engine.begin() as conn:
            await conn.execute(
                hl_wallets.update().where(hl_wallets.c.address == ADDRESS).values(tier="focus")
            )
        await runtime.sync_core_subscriptions()
        assert runtime.core_addresses == set()
        assert runtime.ws.subscription_count == 1
    finally:
        await runtime.close_resources()


@async_timeout()
async def test_core_messages_for_different_wallets_are_processed_concurrently(
    engine, settings, clock
):
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    completed: list[str] = []

    async def handle(message) -> None:
        await asyncio.sleep(0.1)
        completed.append(message["data"]["user"])

    runtime.monitor.handle_core_message = handle
    worker = asyncio.create_task(runtime.run_core_messages())
    try:
        for index in range(10):
            address = f"0x{index + 1:040x}"
            assert await runtime._handle_ws_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {"user": address, "clearinghouseStates": []},
                }
            )

        async def wait_all() -> None:
            while len(completed) < 10:
                await asyncio.sleep(0.005)

        await asyncio.wait_for(wait_all(), timeout=0.5)
    finally:
        runtime._stop.set()
        await asyncio.wait_for(worker, timeout=5)
        await runtime.close_resources()

    assert len(completed) == 10


@async_timeout()
async def test_core_messages_for_same_wallet_keep_arrival_order(engine, settings, clock):
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        Health(),
        engine=engine,
        sink=None,
        clock=clock,
    )
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    completed: list[int] = []

    async def handle(message) -> None:
        sequence = message["data"]["sequence"]
        if sequence == 1:
            first_started.set()
            await release_first.wait()
        completed.append(sequence)

    runtime.monitor.handle_core_message = handle
    worker = asyncio.create_task(runtime.run_core_messages())
    try:
        for sequence in (1, 2):
            assert await runtime._handle_ws_message(
                {
                    "channel": "allDexsClearinghouseState",
                    "data": {
                        "user": ADDRESS,
                        "clearinghouseStates": [],
                        "sequence": sequence,
                    },
                }
            )
        await asyncio.wait_for(first_started.wait(), timeout=5)
        await asyncio.sleep(0.05)
        assert completed == []
        release_first.set()

        async def wait_all() -> None:
            while len(completed) < 2:
                await asyncio.sleep(0.005)

        await asyncio.wait_for(wait_all(), timeout=5)
    finally:
        runtime._stop.set()
        await asyncio.wait_for(worker, timeout=5)
        await runtime.close_resources()

    assert completed == [1, 2]
