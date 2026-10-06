import asyncio
import json

import httpx
import pytest
import respx

from fakes import FakeHealth, async_timeout
from tidewatch.collectors.base import TokenBucket
from tidewatch.hyperliquid.client import REST_URL, HyperliquidRestClient


class RecordingBucket:
    """记录每次扣减的请求权重。"""

    def __init__(self) -> None:
        self.weights: list[float] = []

    async def acquire(self, weight: float = 1) -> None:
        self.weights.append(weight)


class DenyingBucket(RecordingBucket):
    """模拟监控预算耗尽，确保请求不会等待或访问网络。"""

    async def try_acquire(self, weight: float = 1) -> bool:
        self.weights.append(weight)
        return False


class ManualClock:
    """让冷却、恢复和服务端窗口共用同一可控单调时钟。"""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)


class WeightedWindowServer:
    """每个十秒窗口超过权重上限时返回 429 的假服务端。"""

    def __init__(self, clock: ManualClock, limit: int) -> None:
        self.clock = clock
        self.limit = limit
        self.window_weights: dict[int, int] = {}
        self.request_count = 0

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        weight = 20 if payload["type"] == "portfolio" else 2
        window = int(self.clock() // 10)
        used = self.window_weights.get(window, 0)
        self.request_count += 1
        if used + weight > self.limit:
            return httpx.Response(429, json={"error": "限流"}, request=request)
        self.window_weights[window] = used + weight
        return httpx.Response(200, json={}, request=request)


async def _close(client: HyperliquidRestClient) -> None:
    await asyncio.wait_for(client.aclose(), timeout=5)


@async_timeout()
async def test_empty_short_window_bucket_limits_startup_burst_with_fake_server():
    clock = ManualClock()
    monitoring = TokenBucket(
        10,
        capacity=100,
        initial_tokens=0,
        monotonic=clock,
        sleep=clock.sleep,
    )
    batch = TokenBucket(
        200 / 60,
        capacity=200 / 6,
        initial_tokens=0,
        monotonic=clock,
        sleep=clock.sleep,
    )
    server = WeightedWindowServer(clock, limit=100)
    transport = httpx.MockTransport(server)
    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        client=httpx.AsyncClient(transport=transport),
        sleep=clock.sleep,
        monotonic=clock,
        burst_window_s=10,
        aimd_min_budget_per_min=120,
        aimd_recovery_interval_s=60,
    )
    try:
        initial = await asyncio.wait_for(
            asyncio.gather(
                *(client.try_info("clearinghouseState", user=f"0x{i:040x}") for i in range(60))
            ),
            timeout=5,
        )
        clock.advance(10)
        filled = await asyncio.wait_for(
            asyncio.gather(
                *(client.try_info("clearinghouseState", user=f"0x{i:040x}") for i in range(60))
            ),
            timeout=5,
        )
    finally:
        await _close(client)

    assert not any(available for available, _ in initial)
    assert sum(available for available, _ in filled) == 50
    assert server.request_count == 50
    assert max(server.window_weights.values()) <= 100


@async_timeout()
async def test_weighted_window_server_triggers_global_cooldown_then_recovers():
    clock = ManualClock()
    monitoring = TokenBucket(10, capacity=100, monotonic=clock, sleep=clock.sleep)
    batch = TokenBucket(200 / 60, capacity=200 / 6, monotonic=clock, sleep=clock.sleep)
    server = WeightedWindowServer(clock, limit=2)
    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        client=httpx.AsyncClient(transport=httpx.MockTransport(server)),
        sleep=clock.sleep,
        monotonic=clock,
    )
    try:
        assert await client.try_info("clearinghouseState", user="0xabc") == (True, {})
        assert await client.try_info("clearinghouseState", user="0xdef") == (False, None)
        assert await client.try_info("clearinghouseState", user="0x123") == (False, None)
        assert await client.try_info("portfolio", budget="batch", user="0x456") == (
            False,
            None,
        )
        assert server.request_count == 2

        clock.advance(10)
        assert await client.try_info("clearinghouseState", user="0x789") == (True, {})
    finally:
        await _close(client)

    assert server.request_count == 3
    assert monitoring.rate_per_s * 60 == pytest.approx(300)
    assert batch.rate_per_s * 60 == pytest.approx(120)


@async_timeout()
async def test_global_429_circuit_allows_one_probe_and_deduplicates_health_and_logs(caplog):
    clock = ManualClock()
    monitoring = TokenBucket(10, capacity=100, monotonic=clock, sleep=clock.sleep)
    batch = TokenBucket(200 / 60, capacity=200 / 6, monotonic=clock, sleep=clock.sleep)
    health = FakeHealth()
    all_started = asyncio.Event()
    release_429 = asyncio.Event()
    probe_started = asyncio.Event()
    release_probe = asyncio.Event()
    requests = 0

    async def server(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests <= 4:
            if requests == 4:
                all_started.set()
            await release_429.wait()
            return httpx.Response(429, json={"error": "限流"}, request=request)
        probe_started.set()
        await release_probe.wait()
        return httpx.Response(200, json={}, request=request)

    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="global",
        health=health,
        client=httpx.AsyncClient(transport=httpx.MockTransport(server)),
        sleep=clock.sleep,
        monotonic=clock,
        burst_window_s=10,
        aimd_min_budget_per_min=120,
        aimd_recovery_interval_s=60,
    )
    try:
        with caplog.at_level("DEBUG", logger="tidewatch.hyperliquid.client"):
            limited = asyncio.gather(
                *(client.try_info("clearinghouseState", user=f"0x{i:040x}") for i in range(4))
            )
            await asyncio.wait_for(all_started.wait(), timeout=1)
            release_429.set()
            results = await asyncio.wait_for(limited, timeout=1)
            assert not any(available for available, _ in results)

            paused = await asyncio.wait_for(
                asyncio.gather(
                    client.try_info("clearinghouseState", user="0xabc"),
                    client.try_info("portfolio", budget="batch", user="0xdef"),
                ),
                timeout=1,
            )
            assert paused == [(False, None), (False, None)]
            assert requests == 4

            clock.advance(1)
            probe = asyncio.create_task(client.try_info("clearinghouseState", user="0xabc"))
            await asyncio.wait_for(probe_started.wait(), timeout=1)
            assert await asyncio.wait_for(
                client.try_info("portfolio", budget="batch", user="0xdef"), timeout=1
            ) == (False, None)
            assert requests == 5
            release_probe.set()
            assert await asyncio.wait_for(probe, timeout=1) == (True, {})
    finally:
        release_429.set()
        release_probe.set()
        await _close(client)

    warnings = [record for record in caplog.records if record.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "冷却 1 秒" in warnings[0].message
    assert "最近 1 分钟 1" in warnings[0].message
    assert warnings[0].exc_info is None
    assert "Traceback" not in caplog.text
    assert len(health.failures) == 1
    assert monitoring.rate_per_s * 60 == pytest.approx(300)
    assert batch.rate_per_s * 60 == pytest.approx(120)


@async_timeout()
async def test_inflight_success_does_not_clear_health_until_probe_succeeds():
    clock = ManualClock()
    monitoring = TokenBucket(10, capacity=100, monotonic=clock, sleep=clock.sleep)
    batch = TokenBucket(200 / 60, capacity=200 / 6, monotonic=clock, sleep=clock.sleep)
    both_started = asyncio.Event()
    limit_reported = asyncio.Event()
    requests = 0

    class Health(FakeHealth):
        async def failure(self, name: str, error: str) -> None:
            await super().failure(name, error)
            limit_reported.set()

    health = Health()

    async def server(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        current = requests
        if current == 2:
            both_started.set()
        if current <= 2:
            await both_started.wait()
        if current == 1:
            return httpx.Response(429, json={"error": "限流"}, request=request)
        if current == 2:
            await limit_reported.wait()
        return httpx.Response(200, json={}, request=request)

    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        health=health,
        client=httpx.AsyncClient(transport=httpx.MockTransport(server)),
        sleep=clock.sleep,
        monotonic=clock,
    )
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                client.try_info("clearinghouseState", user="0xabc"),
                client.try_info("clearinghouseState", user="0xdef"),
            ),
            timeout=1,
        )
        assert results == [(False, None), (True, {})]
        assert len(health.failures) == 1
        assert health.successes == []

        clock.advance(1)
        assert await asyncio.wait_for(
            client.try_info("clearinghouseState", user="0x123"), timeout=1
        ) == (True, {})
    finally:
        await _close(client)

    assert health.successes == ["hl_rest"]


@async_timeout()
async def test_blocking_requests_do_not_reserve_tokens_during_global_cooldown():
    clock = {"value": 0.0}
    cooldown_sleeping = asyncio.Event()
    release_sleep = asyncio.Event()
    monitoring, batch = RecordingBucket(), RecordingBucket()
    calls = 0

    async def sleep(seconds: float) -> None:
        cooldown_sleeping.set()
        await release_sleep.wait()
        clock["value"] += seconds

    async def server(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, json={"error": "限流"}, request=request)

    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        client=httpx.AsyncClient(transport=httpx.MockTransport(server)),
        sleep=sleep,
        monotonic=lambda: clock["value"],
    )
    tasks: list[asyncio.Task] = []
    try:
        assert await client.try_info("clearinghouseState", user="0xabc") == (False, None)
        tasks = [
            asyncio.create_task(client.info("clearinghouseState", user=f"0x{i:040x}"))
            for i in range(3)
        ]
        await asyncio.wait_for(cooldown_sleeping.wait(), timeout=1)
        await asyncio.sleep(0)

        assert monitoring.weights == [2]
        assert calls == 1
    finally:
        for task in tasks:
            task.cancel()
        release_sleep.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await _close(client)


@async_timeout()
async def test_aimd_recovers_ten_percent_per_clean_minute(caplog):
    clock = ManualClock()
    monitoring = TokenBucket(10, capacity=100, monotonic=clock, sleep=clock.sleep)
    batch = TokenBucket(200 / 60, capacity=200 / 6, monotonic=clock, sleep=clock.sleep)
    responses = iter([429, 200, 200, 200, 200])

    async def server(request: httpx.Request) -> httpx.Response:
        status = next(responses)
        return httpx.Response(status, json={} if status == 200 else {"error": "限流"}, request=request)

    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        client=httpx.AsyncClient(transport=httpx.MockTransport(server)),
        monotonic=clock,
        sleep=clock.sleep,
        burst_window_s=10,
        aimd_min_budget_per_min=120,
        aimd_recovery_interval_s=60,
    )
    try:
        with caplog.at_level("INFO", logger="tidewatch.hyperliquid.client"):
            assert await client.try_info("clearinghouseState", user="0xabc") == (False, None)
            clock.advance(1)
            assert await client.try_info("clearinghouseState", user="0xabc") == (True, {})
            clock.advance(59)
            assert await client.try_info("clearinghouseState", user="0xabc") == (True, {})
            assert monitoring.rate_per_s * 60 == pytest.approx(360)
            assert batch.rate_per_s * 60 == pytest.approx(140)
            clock.advance(60)
            assert await client.try_info("clearinghouseState", user="0xabc") == (True, {})
            assert monitoring.rate_per_s * 60 == pytest.approx(420)
            assert batch.rate_per_s * 60 == pytest.approx(160)
            clock.advance(600)
            assert await client.try_info("clearinghouseState", user="0xabc") == (True, {})
    finally:
        await _close(client)

    assert monitoring.rate_per_s * 60 == pytest.approx(600)
    assert batch.rate_per_s * 60 == pytest.approx(200)
    assert any("有效速率" in record.message for record in caplog.records)


@async_timeout()
async def test_later_inflight_429_restarts_clean_minute_without_repeated_decrease():
    clock = ManualClock()
    monitoring = TokenBucket(10, capacity=100, monotonic=clock, sleep=clock.sleep)
    batch = TokenBucket(200 / 60, capacity=200 / 6, monotonic=clock, sleep=clock.sleep)
    both_started = asyncio.Event()
    release_late = asyncio.Event()
    first_limited = asyncio.Event()
    requests = 0

    class Health(FakeHealth):
        async def failure(self, name: str, error: str) -> None:
            await super().failure(name, error)
            first_limited.set()

    async def server(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        current = requests
        if current == 2:
            both_started.set()
        if current <= 2:
            await both_started.wait()
            if current == 2:
                await release_late.wait()
            return httpx.Response(429, json={"error": "限流"}, request=request)
        return httpx.Response(200, json={}, request=request)

    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        health=Health(),
        client=httpx.AsyncClient(transport=httpx.MockTransport(server)),
        monotonic=clock,
        sleep=clock.sleep,
    )
    try:
        calls = asyncio.gather(
            client.try_info("clearinghouseState", user="0xabc"),
            client.try_info("clearinghouseState", user="0xdef"),
        )
        await asyncio.wait_for(first_limited.wait(), timeout=1)
        clock.advance(0.5)
        release_late.set()
        assert not any(available for available, _ in await asyncio.wait_for(calls, timeout=1))
        assert monitoring.rate_per_s * 60 == pytest.approx(300)

        clock.advance(59.5)
        assert await client.try_info("clearinghouseState", user="0x123") == (True, {})
        assert monitoring.rate_per_s * 60 == pytest.approx(300)
        assert batch.rate_per_s * 60 == pytest.approx(120)

        clock.advance(0.5)
        assert await client.try_info("clearinghouseState", user="0x456") == (True, {})
    finally:
        release_late.set()
        await _close(client)

    assert monitoring.rate_per_s * 60 == pytest.approx(360)
    assert batch.rate_per_s * 60 == pytest.approx(140)


@async_timeout()
async def test_large_response_extra_weight_is_acquired_in_capacity_sized_chunks():
    clock = ManualClock()
    monitoring = TokenBucket(10, capacity=100, monotonic=clock, sleep=clock.sleep)
    batch = TokenBucket(200 / 60, capacity=200 / 6, monotonic=clock, sleep=clock.sleep)
    statuses = iter([429, 429, 429, 200])

    async def server(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if payload["type"] == "clearinghouseState":
            status = next(statuses)
            return httpx.Response(status, json={} if status == 200 else {"error": "限流"}, request=request)
        return httpx.Response(
            200,
            json=[{"tid": index} for index in range(420)],
            request=request,
        )

    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        client=httpx.AsyncClient(transport=httpx.MockTransport(server)),
        monotonic=clock,
        sleep=clock.sleep,
    )
    try:
        await asyncio.wait_for(client.info("clearinghouseState", user="0xabc"), timeout=1)
        assert monitoring.capacity == 20
        fills = await asyncio.wait_for(
            client.info("userFillsByTime", user="0xabc", startTime=0), timeout=1
        )
    finally:
        await _close(client)

    assert len(fills) == 420


@async_timeout()
async def test_extra_weight_chunks_remain_valid_when_bucket_shrinks_mid_acquire():
    class ShrinkingBucket:
        def __init__(self) -> None:
            self.capacity = 50.0
            self.weights: list[float] = []

        async def acquire(self, weight: float) -> None:
            self.weights.append(weight)
            if len(self.weights) == 1:
                self.capacity = 20.0
            if weight > self.capacity:
                raise ValueError("扣减段超过动态桶容量")

    bucket = ShrinkingBucket()

    await HyperliquidRestClient._acquire_weight(bucket, 100)

    assert bucket.weights == [20, 20, 20, 20, 20]


@pytest.mark.parametrize(
    "payload",
    [
        "Too many requests: rate limit exceeded",
        {"status": "err", "response": "Too many requests: rate limit exceeded"},
    ],
)
@respx.mock
@async_timeout()
async def test_explicit_rate_limit_response_shapes_open_global_circuit(payload):
    monitoring, batch = RecordingBucket(), RecordingBucket()
    route = respx.post(REST_URL).mock(return_value=httpx.Response(200, json=payload))
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct")
    try:
        assert await client.try_info("clearinghouseState", user="0xabc") == (False, None)
        assert await client.try_info("clearinghouseState", user="0xdef") == (False, None)
    finally:
        await _close(client)

    assert route.call_count == 1


@respx.mock
@async_timeout()
async def test_explicit_rate_limit_payload_opens_global_circuit():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    health = FakeHealth()
    route = respx.post(REST_URL).mock(
        return_value=httpx.Response(200, json={"error": "Too many requests: rate limit exceeded"})
    )
    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        health=health,
    )
    try:
        assert await asyncio.wait_for(
            client.try_info("clearinghouseState", user="0xabc"), timeout=5
        ) == (False, None)
        assert await asyncio.wait_for(
            client.try_info("clearinghouseState", user="0xdef"), timeout=5
        ) == (False, None)
    finally:
        await _close(client)

    assert route.call_count == 1
    assert len(health.failures) == 1


@respx.mock
@async_timeout()
async def test_info_posts_expected_body_and_preserves_numeric_strings():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    health = FakeHealth()
    fills = [{"px": "123.4500", "sz": "0.010", "tid": index} for index in range(21)]
    route = respx.post(REST_URL).mock(return_value=httpx.Response(200, json=fills))
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct", health=health)
    try:
        result = await asyncio.wait_for(
            client.info("userFillsByTime", user="0xabc", startTime=123),
            timeout=5,
        )
    finally:
        await _close(client)

    assert route.calls[0].request.read() == b'{"type":"userFillsByTime","user":"0xabc","startTime":123}'
    assert monitoring.weights == [20, 1]
    assert batch.weights == []
    assert result[0]["px"] == "123.4500" and result[0]["sz"] == "0.010"
    assert health.successes == ["hl_rest"] and health.failures == []


@respx.mock
@async_timeout()
async def test_info_uses_selected_budget_without_borrowing_from_other_bucket():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    respx.post(REST_URL).mock(return_value=httpx.Response(200, json={"accountValueHistory": []}))
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct")
    try:
        await asyncio.wait_for(client.info("portfolio", budget="batch", user="0xabc"), timeout=5)
    finally:
        await _close(client)

    assert monitoring.weights == []
    assert batch.weights == [20]


@respx.mock
@async_timeout()
async def test_user_abstraction_uses_official_default_info_weight():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    respx.post(REST_URL).mock(return_value=httpx.Response(200, json="disabled"))
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct")
    try:
        await asyncio.wait_for(
            client.info("userAbstraction", budget="batch", user="0xabc"),
            timeout=5,
        )
    finally:
        await _close(client)

    assert monitoring.weights == []
    assert batch.weights == [20]


@respx.mock
@async_timeout()
async def test_try_info_returns_immediately_without_network_when_budget_is_exhausted():
    monitoring, batch = DenyingBucket(), RecordingBucket()
    route = respx.post(REST_URL).mock(return_value=httpx.Response(200, json={}))
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct")
    try:
        result = await asyncio.wait_for(
            client.try_info("clearinghouseState", user="0xabc"),
            timeout=5,
        )
    finally:
        await _close(client)

    assert result == (False, None)
    assert monitoring.weights == [2]
    assert not route.called


@respx.mock
@async_timeout()
async def test_try_info_returns_transient_failure_to_priority_scheduler_without_retrying():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    route = respx.post(REST_URL).mock(return_value=httpx.Response(503))
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct")
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await asyncio.wait_for(
                client.try_info("clearinghouseState", user="0xabc"),
                timeout=5,
            )
    finally:
        await _close(client)

    assert route.call_count == 1
    assert monitoring.weights == [2]


@respx.mock
@async_timeout()
async def test_info_retries_429_server_and_network_errors_with_exponential_backoff(caplog):
    monitoring, batch = RecordingBucket(), RecordingBucket()
    health = FakeHealth()
    clock = ManualClock()

    route = respx.post(REST_URL)
    route.side_effect = [
        httpx.Response(429, json={"error": "limited"}),
        httpx.Response(503, json={"error": "busy"}),
        httpx.ConnectError("网络断开"),
        httpx.Response(200, json={"marginSummary": {"accountValue": "1000.00"}}),
    ]
    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="global",
        proxy_url="http://127.0.0.1:10808",
        health=health,
        sleep=clock.sleep,
        monotonic=clock,
    )
    try:
        with caplog.at_level("WARNING", logger="tidewatch.hyperliquid.client"):
            result = await asyncio.wait_for(client.info("clearinghouseState", user="0xabc"), timeout=5)
    finally:
        await _close(client)

    assert result["marginSummary"]["accountValue"] == "1000.00"
    assert clock.sleeps == [1, 1, 2]
    assert monitoring.weights == [2, 2, 2, 2]
    assert len(health.failures) == 3 and health.successes == ["hl_rest"]
    assert any("hl_rest" in record.message and "clearinghouseState" in record.message for record in caplog.records)
    assert any("global" in record.message and "1 秒" in record.message for record in caplog.records)


@respx.mock
@async_timeout()
async def test_info_resets_backoff_after_success():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    route = respx.post(REST_URL)
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(200, json={}),
        httpx.Response(503),
        httpx.Response(200, json={}),
    ]
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct", sleep=fake_sleep)
    try:
        await asyncio.wait_for(client.info("clearinghouseState", user="0xabc"), timeout=5)
        await asyncio.wait_for(client.info("clearinghouseState", user="0xabc"), timeout=5)
    finally:
        await _close(client)

    assert sleeps == [1, 2, 4, 8, 1]


@respx.mock
@async_timeout()
async def test_info_caps_retry_delay_at_five_minutes():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    clock = ManualClock()

    route = respx.post(REST_URL)
    route.side_effect = [*[httpx.Response(429) for _ in range(11)], httpx.Response(200, json={})]
    client = HyperliquidRestClient(
        monitoring,
        batch,
        proxy_mode="direct",
        sleep=clock.sleep,
        monotonic=clock,
    )
    try:
        await asyncio.wait_for(client.info("spotClearinghouseState", user="0xabc"), timeout=5)
    finally:
        await _close(client)

    assert clock.sleeps == [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300]


@respx.mock
@async_timeout()
async def test_info_does_not_retry_other_client_errors():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    health = FakeHealth()
    respx.post(REST_URL).mock(return_value=httpx.Response(400, json={"error": "bad request"}))
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct", health=health)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await asyncio.wait_for(client.info("clearinghouseState", user="0xabc"), timeout=5)
    finally:
        await _close(client)

    assert monitoring.weights == [2]
    assert len(health.failures) == 1 and health.successes == []


@pytest.mark.parametrize(
    ("proxy_mode", "proxy_url", "expected_proxy"),
    [
        ("global", "http://127.0.0.1:10808", "http://127.0.0.1:10808"),
        ("global", None, None),
        ("direct", "http://127.0.0.1:10808", None),
    ],
)
def test_rest_proxy_selection(monkeypatch, proxy_mode, proxy_url, expected_proxy):
    seen: list[tuple[str, str | None]] = []

    class StubClient:
        async def aclose(self) -> None:
            return None

    def fake_make_client(mode: str, url: str | None, *, timeout: float = 20.0):
        seen.append((mode, url if mode == "global" else None))
        return StubClient()

    monkeypatch.setattr("tidewatch.hyperliquid.client.make_client", fake_make_client)
    client = HyperliquidRestClient(RecordingBucket(), RecordingBucket(), proxy_mode=proxy_mode, proxy_url=proxy_url)

    assert seen == [(proxy_mode, expected_proxy)]
    asyncio.run(_close(client))
