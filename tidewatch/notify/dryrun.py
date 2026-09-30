"""演练通知器：不发送任何消息，只写日志（--dry-run 使用）"""

from __future__ import annotations

import logging
from typing import Any

from tidewatch.notify.base import EventCard, Notifier, SendResult

logger = logging.getLogger(__name__)


class DryRunNotifier(Notifier):
    status_on_success = "dry_run"

    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        logger.info("【演练】%s 级推送：%s", card.level, card.title)
        self.sent.append(("event", card))
        return SendResult(True, detail="dry_run")

    async def send_digest(self, cards: list[EventCard]) -> SendResult:
        logger.info("【演练】A 级汇总 %d 条", len(cards))
        self.sent.append(("digest", cards))
        return SendResult(True, detail="dry_run")

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        logger.info("【演练】健康通知 %s：%s", source, detail)
        self.sent.append(("health", (source, healthy, detail)))
        return SendResult(True, detail="dry_run")
