"""演练通知器：不发送任何消息，只写日志（--dry-run 使用）"""

from __future__ import annotations

import logging
from collections import deque
from typing import Any

from tidewatch.notify.base import EventCard, Notifier, SendResult

logger = logging.getLogger(__name__)

# 最多保留的演练记录条数，超出后丢弃最旧的，避免长期运行时内存无限增长
_MAX_RECORDS = 1000


class DryRunNotifier(Notifier):
    """演练通知器：所有发送都视为成功，只写日志并记录在内存中，不访问网络

    发送成功时 push_log 状态为 dry_run，以便与真实推送区分。
    self.sent 保存最近 _MAX_RECORDS 条 (类型, 内容) 记录，类型为 event / digest / health。
    """

    status_on_success = "dry_run"

    def __init__(self) -> None:
        """初始化演练记录队列（有界，仅保留最近 1000 条）"""
        self.sent: deque[tuple[str, Any]] = deque(maxlen=_MAX_RECORDS)

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        """记录一条事件推送（不发送）"""
        logger.info("【演练】%s 级推送：%s（mention_all=%s）", card.level, card.title, mention_all)
        self.sent.append(("event", card))
        return SendResult(True, detail="dry_run")

    async def send_digest(self, cards: list[EventCard]) -> SendResult:
        """记录一次 A 级汇总（不发送）"""
        logger.info("【演练】A 级汇总 %d 条", len(cards))
        self.sent.append(("digest", cards))
        return SendResult(True, detail="dry_run")

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        """记录一次健康通知（不发送），日志中标明状态为正常或异常"""
        logger.info("【演练】健康通知 %s（%s）：%s", source, "正常" if healthy else "异常", detail)
        self.sent.append(("health", (source, healthy, detail)))
        return SendResult(True, detail="dry_run")
