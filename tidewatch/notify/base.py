"""推送通道接口（系统设计 9.1）"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

SOURCE_NAMES = {"jin10": "金十快讯", "binance_ann": "币安公告", "okx_ann": "OKX 公告"}


def source_name(source: str) -> str:
    """采集器名 → 中文显示名"""
    return SOURCE_NAMES.get(source, source)


@dataclass
class EventCard:
    """一条待推送事件的展示内容"""

    push_id: int
    cluster_id: int
    level: str
    title: str
    source: str
    occurred_at: datetime
    url: str | None
    assets: list[str]
    body: str
    source_count: int
    upgraded: bool


@dataclass
class SendResult:
    ok: bool
    retryable: bool = False
    detail: str = ""


class Notifier(ABC):
    """推送通道；status_on_success 为发送成功时写入 push_log 的状态"""

    status_on_success: str = "ok"

    @abstractmethod
    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult: ...

    @abstractmethod
    async def send_digest(self, cards: list[EventCard]) -> SendResult: ...

    @abstractmethod
    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult: ...

    async def aclose(self) -> None:
        """释放连接等资源（默认无需处理）"""
