import asyncio

import httpx
import pytest
import respx

from fakes import FakeHealth
from tidewatch.hyperliquid.client import REST_URL, HyperliquidRestClient


class RecordingBucket:
    """记录每次扣减的请求权重。"""

    def __init__(self) -> None:
        self.weights: list[float] = []

    async def acquire(self, weight: float = 1) -> None:
        self.weights.append(weight)


async def _close(client: HyperliquidRestClient) -> None:
    await asyncio.wait_for(client.aclose(), timeout=5)


@respx.mock
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
async def test_info_retries_429_server_and_network_errors_with_exponential_backoff(caplog):
    monitoring, batch = RecordingBucket(), RecordingBucket()
    health = FakeHealth()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

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
        sleep=fake_sleep,
    )
    try:
        with caplog.at_level("WARNING", logger="tidewatch.hyperliquid.client"):
            result = await asyncio.wait_for(client.info("clearinghouseState", user="0xabc"), timeout=5)
    finally:
        await _close(client)

    assert result["marginSummary"]["accountValue"] == "1000.00"
    assert sleeps == [1, 2, 4]
    assert monitoring.weights == [2, 2, 2, 2]
    assert len(health.failures) == 3 and health.successes == ["hl_rest"]
    assert any("hl_rest" in record.message and "clearinghouseState" in record.message for record in caplog.records)
    assert any("global" in record.message and "1 秒" in record.message for record in caplog.records)


@respx.mock
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
async def test_info_caps_retry_delay_at_five_minutes():
    monitoring, batch = RecordingBucket(), RecordingBucket()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    route = respx.post(REST_URL)
    route.side_effect = [*[httpx.Response(429) for _ in range(11)], httpx.Response(200, json={})]
    client = HyperliquidRestClient(monitoring, batch, proxy_mode="direct", sleep=fake_sleep)
    try:
        await asyncio.wait_for(client.info("spotClearinghouseState", user="0xabc"), timeout=5)
    finally:
        await _close(client)

    assert sleeps == [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300]


@respx.mock
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
