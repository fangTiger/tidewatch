import asyncio
from datetime import datetime, timezone

import pytest

from fakes import FakeHealth, FakeSink, FakeSleep
from tidewatch.collectors.base import Backoff, PollCollector, StreamCollector, TokenBucket
from tidewatch.collectors.http import make_client
from tidewatch.config import CollectorCfg
from tidewatch.models import Event, FetchResult

CFG = CollectorCfg(interval_s=20, stale_after_s=300, gap_fill_interval_s=600, rate_per_s=2)
NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


def _event(key: str) -> Event:
    return Event(source="demo", category="news", event_type="flash", occurred_at=NOW, title=key, dedupe_key=key)


def test_backoff_sequence():
    backoff = Backoff()
    assert [backoff.next() for _ in range(10)] == [1, 2, 4, 8, 16, 32, 64, 128, 256, 300]
    backoff.reset()
    assert backoff.next() == 1


async def test_token_bucket_waits_when_empty():
    clock = {"t": 0.0}
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["t"] += seconds

    bucket = TokenBucket(2, monotonic=lambda: clock["t"], sleep=fake_sleep)
    for _ in range(3):
        await bucket.acquire()
    assert sleeps == [pytest.approx(0.5)]


class DemoPoll(PollCollector):
    name = "demo"

    def __init__(self, **kwargs) -> None:
        super().__init__(CFG, **kwargs)
        self.seen: list[dict | None] = []

    async def load_cursor(self):
        return None

    async def fetch(self, cursor):
        self.seen.append(cursor)
        n = len(self.seen)
        return FetchResult([_event(f"e{n}")], {"n": n})


async def test_poll_keeps_old_cursor_when_ingest_fails():
    sink, health, sleep = FakeSink(fail_on={2}), FakeHealth(), FakeSleep(stop_after=3)
    collector = DemoPoll(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    assert collector.seen == [None, {"n": 1}, {"n": 1}]
    assert sleep.delays == [20, 1, 20]
    assert len(health.failures) == 1 and "数据库暂时不可用" in health.failures[0][1]


class BrokenHealth(FakeHealth):
    async def failure(self, name: str, error: str) -> None:
        raise RuntimeError("数据库断开，健康状态写不进去")


async def test_poll_survives_health_report_failure():
    sink, health, sleep = FakeSink(fail_on={1}), BrokenHealth(), FakeSleep(stop_after=2)
    collector = DemoPoll(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    assert collector.seen == [None, None]  # 第一轮失败后保持旧游标并继续
    assert sleep.delays == [1, 20]


class DemoStream(StreamCollector):
    name = "demo_ws"

    def __init__(self, **kwargs) -> None:
        super().__init__(CFG, **kwargs)
        self.gap_cursors: list[dict | None] = []

    async def load_cursor(self):
        return {"last_ms": 1}

    async def gap_fill(self, cursor):
        self.gap_cursors.append(cursor)
        return FetchResult([], {"last_ms": 5})

    async def stream(self, cursor):
        yield FetchResult([], None)  # 心跳
        yield FetchResult([_event("ws1")], {"last_ms": 9})

    def merge_cursor(self, old, new):
        if new is None:
            return old
        return {"last_ms": max((old or {}).get("last_ms", 0), new["last_ms"])}


async def test_stream_gap_fill_then_stream_then_reconnect():
    sink, health, sleep = FakeSink(), FakeHealth(), FakeSleep(stop_after=1)
    collector = DemoStream(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    assert collector.gap_cursors == [{"last_ms": 1}]
    ingested = [result for _, result in sink.calls]
    assert [len(r.events) for r in ingested] == [0, 1]  # 补缺结果 + WS 事件；心跳不入库
    assert collector.cursor == {"last_ms": 9}
    assert len(health.successes) == 2  # 只在收到长连接消息时上报成功
    assert health.failures and "长连接" in health.failures[0][1]  # 连接结束后按失败处理并退避
    assert sleep.delays == [1]


def test_make_client_proxy_modes():
    direct = make_client("direct", "http://127.0.0.1:10808")
    proxied = make_client("global", "http://127.0.0.1:10808")
    no_proxy_configured = make_client("global", None)
    assert not direct._mounts and proxied._mounts and not no_proxy_configured._mounts
