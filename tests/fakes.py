"""测试替身"""

from __future__ import annotations

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
