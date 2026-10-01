"""测试替身"""

from __future__ import annotations

import asyncio
import socket
from typing import Callable

from tidewatch.notify.base import EventCard, Notifier, SendResult


class FakeNotifier(Notifier):
    """记录所有调用；results 为预设的发送结果队列，用完后一律成功"""

    status_on_success = "ok"

    def __init__(self, results: list[SendResult] | None = None) -> None:
        self.results = list(results or [])
        self.events: list[tuple[EventCard, bool]] = []
        self.digests: list[list[EventCard]] = []
        self.health: list[tuple[str, bool, str]] = []

    def _next(self) -> SendResult:
        return self.results.pop(0) if self.results else SendResult(True, detail="code=0")

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        self.events.append((card, mention_all))
        return self._next()

    async def send_digest(self, cards: list[EventCard]) -> SendResult:
        self.digests.append(list(cards))
        return self._next()

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        self.health.append((source, healthy, detail))
        return self._next()


from tidewatch.models import FetchResult


class FakeSink:
    """记录 ingest 调用；fail_on 为需要抛异常的调用序号（从 1 开始）"""

    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.calls: list[tuple[str, FetchResult]] = []
        self.fail_on = fail_on or set()

    async def ingest(self, result: FetchResult, *, collector: str) -> list[int]:
        self.calls.append((collector, result))
        if len(self.calls) in self.fail_on:
            raise RuntimeError("数据库暂时不可用")
        return list(range(len(result.events)))


class FakeHealth:
    def __init__(self) -> None:
        self.successes: list[str] = []
        self.failures: list[tuple[str, str]] = []

    async def success(self, name: str) -> None:
        self.successes.append(name)

    async def failure(self, name: str, error: str) -> None:
        self.failures.append((name, error))


class FakeSleep:
    """记录等待时长；第 stop_after 次调用时抛 CancelledError，用于结束无限循环"""

    def __init__(self, stop_after: int) -> None:
        self.delays: list[float] = []
        self.stop_after = stop_after

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        if len(self.delays) >= self.stop_after:
            raise asyncio.CancelledError


def free_port() -> int:
    """取一个当前空闲的本机端口（供真实 uvicorn 的测试使用）"""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    """轮询等待条件成立；超时抛 TimeoutError，避免测试挂起"""

    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.02)

    await asyncio.wait_for(poll(), timeout)
