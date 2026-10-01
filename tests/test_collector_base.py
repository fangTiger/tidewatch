import asyncio
from datetime import datetime, timezone

import pytest

from fakes import FakeHealth, FakeNotifier, FakeSink, FakeSleep
from tidewatch.collectors.base import Backoff, PollCollector, StreamCollector, TokenBucket, describe
from tidewatch.collectors.http import make_client
from tidewatch.config import CollectorCfg
from tidewatch.health import HealthMonitor
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


# ---------------------------------------------------------------------------
# 测试辅助
# ---------------------------------------------------------------------------


async def _until(predicate, timeout: float = 5.0) -> None:
    """轮询直到条件成立，超时抛 TimeoutError，防止测试挂起"""

    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout=timeout)


def _stream(cls, sleep, *, sink=None, health=None, **kwargs):
    return cls(sink=sink or FakeSink(), health=health or FakeHealth(), client=None, engine=None, sleep=sleep, **kwargs)


# ---------------------------------------------------------------------------
# describe：消息为空的异常用 repr
# ---------------------------------------------------------------------------


def test_describe_uses_repr_when_message_is_empty():
    assert describe(RuntimeError("boom")) == "RuntimeError：boom"
    assert describe(asyncio.TimeoutError()) == "TimeoutError()"
    assert describe(ConnectionResetError()) == "ConnectionResetError()"
    assert len(describe(RuntimeError("x" * 500))) == 300


# ---------------------------------------------------------------------------
# 轮询：只有入库成功才上报成功
# ---------------------------------------------------------------------------


async def test_poll_reports_success_only_after_ingest_succeeds():
    sink, health, sleep = FakeSink(fail_on={1, 2, 3}), FakeHealth(), FakeSleep(stop_after=3)
    collector = DemoPoll(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert health.successes == []  # 入库失败的轮次不能上报成功
    assert len(health.failures) == 3 and all("数据库暂时不可用" in err for _, err in health.failures)


async def test_poll_reports_success_once_per_successful_round():
    sink, health, sleep = FakeSink(fail_on={2}), FakeHealth(), FakeSleep(stop_after=3)
    collector = DemoPoll(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert len(health.successes) == 2 and len(health.failures) == 1


async def test_poll_ingest_failures_turn_real_health_abnormal(engine, settings, clock):
    """用真实 HealthMonitor 验证：连续入库失败不会被「先上报成功」清零，5 轮后进入 abnormal"""
    notifier = FakeNotifier()
    monitor = HealthMonitor(engine, notifier, settings.dispatch, clock=clock)
    monitor.register("demo", stale_after_s=300)
    sink, sleep = FakeSink(fail_on={1, 2, 3, 4, 5}), FakeSleep(stop_after=5)
    collector = DemoPoll(sink=sink, health=monitor, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=10)
    assert monitor.status("demo") == "abnormal"
    assert [(source, ok) for source, ok, _ in notifier.health] == [("demo", False)]


class CancelledFetchPoll(DemoPoll):
    async def fetch(self, cursor):
        raise asyncio.CancelledError


async def test_poll_fetch_cancelled_error_propagates_without_recording_failure():
    health, sleep = FakeHealth(), FakeSleep(stop_after=5)
    collector = CancelledFetchPoll(sink=FakeSink(), health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert health.failures == [] and health.successes == [] and sleep.delays == []


# ---------------------------------------------------------------------------
# 读取游标重试后重置退避
# ---------------------------------------------------------------------------


class FlakyCursorPoll(DemoPoll):
    """前 3 次读取游标失败"""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.load_calls = 0

    async def load_cursor(self):
        self.load_calls += 1
        if self.load_calls <= 3:
            raise RuntimeError("数据库暂时不可用")
        return None


async def test_load_cursor_retry_resets_backoff_after_success():
    # 第 1 轮采集入库失败：退避应从 1 秒重新开始，而不是接着读取游标时累积的 8 秒
    sink, health, sleep = FakeSink(fail_on={1}), FakeHealth(), FakeSleep(stop_after=4)
    collector = FlakyCursorPoll(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert sleep.delays == [1, 2, 4, 1]


# ---------------------------------------------------------------------------
# 长连接：只有 _commit 成功才上报成功；心跳无事件直接上报
# ---------------------------------------------------------------------------


class EventOnlyStream(DemoStream):
    """只推送一条带事件的消息就断开"""

    async def stream(self, cursor):
        yield FetchResult([_event("ws1")], {"last_ms": 9})


class HeartbeatOnlyStream(DemoStream):
    """只推送一次心跳就断开"""

    async def stream(self, cursor):
        yield FetchResult([], None)


class FailingStream(DemoStream):
    """连接立即失败；补缺（REST）每次都成功"""

    async def stream(self, cursor):
        raise ConnectionError("连不上")
        yield  # pragma: no cover


async def test_stream_does_not_report_success_when_commit_fails():
    sink, health = FakeSink(fail_on={2}), FakeHealth()  # 第 1 次入库是补缺，第 2 次是长连接消息
    collector = _stream(EventOnlyStream, FakeSleep(stop_after=1), sink=sink, health=health)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert health.successes == []
    assert len(health.failures) == 1 and "数据库暂时不可用" in health.failures[0][1]
    assert collector.cursor == {"last_ms": 5}  # 入库失败不推进游标


async def test_stream_heartbeat_without_events_reports_success_directly():
    sink, health = FakeSink(), FakeHealth()
    collector = _stream(HeartbeatOnlyStream, FakeSleep(stop_after=1), sink=sink, health=health)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert len(sink.calls) == 1  # 只有补缺入库，心跳不入库
    assert len(health.successes) == 1 and len(health.failures) == 1


async def test_stream_gap_fill_success_alone_does_not_report_success():
    """补缺成功不代表长连接可用：长连接持续失败时，补缺成功不能把连续失败清零"""
    sink, health, sleep = FakeSink(), FakeHealth(), FakeSleep(stop_after=3)
    collector = _stream(FailingStream, sleep, sink=sink, health=health)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert len(collector.gap_cursors) == 3 and len(sink.calls) == 3  # 每次重连前都补缺并入库
    assert health.successes == [] and len(health.failures) == 3
    assert sleep.delays == [1, 2, 4]


# ---------------------------------------------------------------------------
# 长连接：外部取消不被吞掉
# ---------------------------------------------------------------------------


class SlowCleanupStream(DemoStream):
    """连接立即断开；补缺后台任务被取消后，清理需要一点时间"""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.attempts = 0
        self.filler_running = asyncio.Event()
        self.cleanup_started = asyncio.Event()

    async def _gap_fill_loop(self) -> None:
        self.filler_running.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cleanup_started.set()
            await asyncio.sleep(0.05)  # 模拟清理耗时（真实等待）
            raise

    async def stream(self, cursor):
        self.attempts += 1
        await self.filler_running.wait()  # 保证补缺任务已开始运行，取消时才会进入清理
        yield FetchResult([], None)


async def test_stream_single_external_cancel_ends_run_during_filler_cleanup():
    # FakeSleep 只作保险：若取消被吞掉，循环最终也会在第 3 次退避时结束，不会挂起
    collector = _stream(SlowCleanupStream, FakeSleep(stop_after=3))
    task = asyncio.ensure_future(collector.run())
    try:
        await asyncio.wait_for(collector.cleanup_started.wait(), timeout=5)
        task.cancel()  # 外部只取消一次
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
        assert collector.attempts == 1  # 取消后不得再次重连
    finally:
        task.cancel()
        await asyncio.wait({task}, timeout=5)


# ---------------------------------------------------------------------------
# 长连接：_commit 串行化，游标不回退
# ---------------------------------------------------------------------------


class OrderedSink:
    """第 1 次入库让出一次事件循环后返回；第 2 次入库要等第 1 次结束才返回（模拟较旧结果入库更慢）"""

    def __init__(self) -> None:
        self.calls: list[FetchResult] = []
        self.first_done = asyncio.Event()

    async def ingest(self, result: FetchResult, *, collector: str) -> list[int]:
        self.calls.append(result)
        if len(self.calls) == 1:
            try:
                await asyncio.sleep(0)
            finally:
                self.first_done.set()
            return []
        await self.first_done.wait()
        await asyncio.sleep(0)
        return []


async def test_stream_concurrent_commits_never_regress_cursor():
    sink = OrderedSink()
    collector = _stream(DemoStream, FakeSleep(stop_after=1), sink=sink)
    newer = FetchResult([_event("a")], {"last_ms": 9})
    older = FetchResult([_event("b")], {"last_ms": 5})
    await asyncio.wait_for(asyncio.gather(collector._commit(newer), collector._commit(older)), timeout=5)
    assert collector.cursor == {"last_ms": 9}
    assert [r.cursor for r in sink.calls] == [{"last_ms": 9}, {"last_ms": 9}]  # 入库的游标也不回退


# ---------------------------------------------------------------------------
# 长连接：退避只在「连接足够久」或「收到含事件的数据」时重置
# ---------------------------------------------------------------------------


async def test_stream_heartbeat_only_does_not_reset_backoff():
    sleep = FakeSleep(stop_after=3)
    collector = _stream(HeartbeatOnlyStream, sleep)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert sleep.delays == [1, 2, 4]


class EventsEveryTimeStream(DemoStream):
    async def stream(self, cursor):
        yield FetchResult([], None)  # 先心跳
        yield FetchResult([_event("ws")], {"last_ms": 9})


async def test_stream_event_data_resets_backoff():
    sleep = FakeSleep(stop_after=3)
    collector = _stream(EventsEveryTimeStream, sleep)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert sleep.delays == [1, 1, 1]


class TimedStream(DemoStream):
    """连接持续 elapsed 秒（推进注入的单调时钟）；heartbeat 为 True 时先发一次心跳再断开，否则直接断开"""

    def __init__(self, *, mono: dict, elapsed: float, heartbeat: bool, **kwargs) -> None:
        super().__init__(monotonic=lambda: mono["t"], **kwargs)
        self._mono, self._elapsed, self._heartbeat = mono, elapsed, heartbeat

    async def stream(self, cursor):
        self._mono["t"] += self._elapsed
        if self._heartbeat:
            yield FetchResult([], None)


@pytest.mark.parametrize("heartbeat", [True, False])
@pytest.mark.parametrize(("elapsed", "expected"), [(59.9, [1, 2, 4]), (60, [1, 1, 1]), (3600, [1, 1, 1])])
async def test_stream_backoff_resets_only_after_connection_lasted_60s(elapsed, expected, heartbeat):
    sleep = FakeSleep(stop_after=3)
    collector = _stream(TimedStream, sleep, mono={"t": 1000.0}, elapsed=elapsed, heartbeat=heartbeat)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(collector.run(), timeout=5)
    assert sleep.delays == expected


# ---------------------------------------------------------------------------
# 长连接：定时补缺后台任务确实在运行
# ---------------------------------------------------------------------------


class TickSleep:
    """前 ticks 次等待让出一次事件循环后返回，之后永久等待：只放行有限次定时补缺"""

    def __init__(self, ticks: int) -> None:
        self.ticks = ticks
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        if len(self.calls) > self.ticks:
            await asyncio.Event().wait()
        await asyncio.sleep(0)


class IdleStream(DemoStream):
    """长连接一直没有消息"""

    async def stream(self, cursor):
        await asyncio.Event().wait()
        yield FetchResult([], None)  # pragma: no cover


async def test_stream_periodic_gap_fill_runs_in_background():
    sink, sleep = FakeSink(), TickSleep(ticks=1)
    collector = _stream(IdleStream, sleep, sink=sink)
    task = asyncio.ensure_future(collector.run())
    try:
        await _until(lambda: len(sink.calls) >= 2)  # 第 1 次是连接前补缺，第 2 次来自定时补缺任务
    finally:
        task.cancel()
        await asyncio.wait({task}, timeout=5)
    assert task.cancelled()
    assert sleep.calls[0] == 600  # 使用 gap_fill_interval_s
    assert collector.gap_cursors == [{"last_ms": 1}, {"last_ms": 5}]  # 定时补缺基于最新游标
