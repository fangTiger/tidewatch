import asyncio
import json

import pytest
from websockets.asyncio.server import serve

from fakes import FakeHealth
from tidewatch.hyperliquid.ws import WS_URL, HyperliquidWsManager


async def _stop(manager: HyperliquidWsManager, task: asyncio.Task) -> None:
    await asyncio.wait_for(manager.aclose(), timeout=5)
    await asyncio.wait_for(task, timeout=5)


def test_ws_constants_and_connection_limit():
    assert WS_URL == "wss://api.hyperliquid.xyz/ws"
    manager = HyperliquidWsManager(proxy_mode="direct")
    for index in range(10):
        manager.add_connection(f"connection-{index}")

    with pytest.raises(ValueError, match="最多 10 个连接"):
        manager.add_connection("connection-10")


def test_ws_total_subscription_limit_and_duplicate_is_idempotent():
    manager = HyperliquidWsManager(proxy_mode="direct")
    manager.add_connection("trades")
    for index in range(1000):
        assert manager.add_subscription("trades", {"type": "trades", "coin": f"COIN-{index}"})

    assert not manager.add_subscription("trades", {"coin": "COIN-0", "type": "trades"})
    with pytest.raises(ValueError, match="总订阅最多 1,000 个"):
        manager.add_subscription("trades", {"type": "trades", "coin": "OVER"})


def test_ws_user_subscription_limit_keeps_existing_subscriptions():
    manager = HyperliquidWsManager(proxy_mode="direct")
    manager.add_connection("core")
    for index in range(10):
        manager.add_subscription("core", {"type": "userFills", "user": f"0x{index:040x}"})

    with pytest.raises(ValueError, match="按用户订阅最多 10 个用户"):
        manager.add_subscription("core", {"type": "userFills", "user": f"0x{10:040x}"})
    assert manager.subscription_count == 10


async def test_ws_sends_protocol_ping_and_reports_valid_pong():
    received: list[dict] = []
    pong_sent = asyncio.Event()
    sleep_started = asyncio.Event()
    release_ping = asyncio.Event()
    sleep_calls: list[float] = []

    async def controlled_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) == 1:
            sleep_started.set()
            await release_ping.wait()
            return
        await asyncio.Event().wait()

    async def handler(websocket) -> None:
        message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))
        received.append(message)
        await asyncio.wait_for(websocket.send(json.dumps({"channel": "pong"})), timeout=5)
        pong_sent.set()
        await asyncio.wait_for(websocket.wait_closed(), timeout=5)

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        health = FakeHealth()
        manager = HyperliquidWsManager(
            url=f"ws://127.0.0.1:{port}",
            proxy_mode="direct",
            health=health,
            sleep=controlled_sleep,
        )
        manager.add_connection("core")
        task = asyncio.create_task(manager.run())
        try:
            await asyncio.wait_for(sleep_started.wait(), timeout=5)
            release_ping.set()
            await asyncio.wait_for(pong_sent.wait(), timeout=5)
        finally:
            await _stop(manager, task)

    assert received == [{"method": "ping"}]
    assert sleep_calls[0] == 50
    assert health.successes == ["hl_core_ws"]


async def test_ws_reconnects_after_disconnect_and_restores_subscription_once():
    connections: list[list[dict]] = []
    restored = asyncio.Event()

    async def handler(websocket) -> None:
        messages: list[dict] = []
        connections.append(messages)
        messages.append(json.loads(await asyncio.wait_for(websocket.recv(), timeout=5)))
        if len(connections) == 1:
            await asyncio.wait_for(websocket.close(), timeout=5)
            return
        restored.set()
        await asyncio.wait_for(websocket.wait_closed(), timeout=5)

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        if seconds == 3600:
            await asyncio.Event().wait()
        sleeps.append(seconds)
        await asyncio.sleep(0)

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        manager = HyperliquidWsManager(
            url=f"ws://127.0.0.1:{port}",
            proxy_mode="direct",
            sleep=fake_sleep,
            ping_interval_s=3600,
        )
        manager.add_connection("core")
        subscription = {"type": "userFills", "user": "0x0000000000000000000000000000000000000001"}
        manager.add_subscription("core", subscription)
        manager.add_subscription("core", subscription)
        task = asyncio.create_task(manager.run())
        try:
            await asyncio.wait_for(restored.wait(), timeout=5)
        finally:
            await _stop(manager, task)

    expected = {"method": "subscribe", "subscription": subscription}
    assert connections == [[expected], [expected]]
    assert sleeps == [1]


@pytest.mark.parametrize(
    ("proxy_mode", "proxy_url", "expected_proxy"),
    [
        ("global", "http://127.0.0.1:10808", "http://127.0.0.1:10808"),
        ("global", None, None),
        ("direct", "http://127.0.0.1:10808", None),
    ],
)
async def test_ws_proxy_selection_is_passed_to_connector(proxy_mode, proxy_url, expected_proxy):
    seen: list[tuple[str, str | None]] = []
    closed = asyncio.Event()

    class StubSocket:
        async def send(self, message: str) -> None:
            return None

        async def close(self) -> None:
            closed.set()

        def __aiter__(self):
            return self

        async def __anext__(self):
            await closed.wait()
            raise StopAsyncIteration

    class StubContext:
        async def __aenter__(self):
            return StubSocket()

        async def __aexit__(self, *args):
            return None

    def connector(url: str, *, proxy: str | None, **kwargs):
        seen.append((url, proxy))
        return StubContext()

    manager = HyperliquidWsManager(
        proxy_mode=proxy_mode,
        proxy_url=proxy_url,
        connect=connector,
        ping_interval_s=3600,
    )
    manager.add_connection("core")
    task = asyncio.create_task(manager.run())
    try:
        async def wait_connected() -> None:
            while not seen:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_connected(), timeout=5)
    finally:
        await _stop(manager, task)

    assert seen == [(WS_URL, expected_proxy)]


async def test_ws_run_without_connections_waits_until_graceful_close():
    manager = HyperliquidWsManager(proxy_mode="direct")
    task = asyncio.create_task(manager.run())
    await asyncio.sleep(0)
    assert not task.done()
    await _stop(manager, task)


async def test_ws_close_interrupts_reconnect_backoff():
    """优雅关闭必须能打断最长可达五分钟的重连等待。"""
    sleeping = asyncio.Event()
    never = asyncio.Event()

    async def blocking_sleep(seconds: float) -> None:
        if seconds == 1:
            sleeping.set()
        await never.wait()

    async def handler(websocket) -> None:
        await websocket.close()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        manager = HyperliquidWsManager(
            url=f"ws://127.0.0.1:{port}", proxy_mode="direct", sleep=blocking_sleep
        )
        manager.add_connection("core")
        task = asyncio.create_task(manager.run())
        await asyncio.wait_for(sleeping.wait(), timeout=5)
        await asyncio.wait_for(manager.aclose(), timeout=5)
        await asyncio.wait_for(task, timeout=5)


async def test_ws_short_lived_messages_do_not_reset_reconnect_backoff():
    """订阅确认后立即断线仍须按 1、2、4 秒递增，而不是永久 1 秒。"""
    connections = 0
    third_connected = asyncio.Event()
    sleeps: list[float] = []

    async def handler(websocket) -> None:
        nonlocal connections
        connections += 1
        await websocket.send(json.dumps({"channel": "subscriptionResponse"}))
        if connections < 3:
            await websocket.close()
        else:
            third_connected.set()
            await websocket.wait_closed()

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await asyncio.sleep(0)

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        manager = HyperliquidWsManager(
            url=f"ws://127.0.0.1:{port}", proxy_mode="direct", sleep=fake_sleep, ping_interval_s=3600
        )
        manager.add_connection("core")
        task = asyncio.create_task(manager.run())
        try:
            await asyncio.wait_for(third_connected.wait(), timeout=5)
        finally:
            await _stop(manager, task)

    assert [delay for delay in sleeps if delay != 3600][:2] == [1, 2]
