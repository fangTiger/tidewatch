"""推送通道接口（系统设计 9.1）"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

SOURCE_NAMES = {
    "jin10": "金十快讯",
    "binance_ann": "币安公告",
    "okx_ann": "OKX 公告",
    "hl_monitor": "Hyperliquid 巨鲸监控",
    "hl_discovery": "Hyperliquid 巨鲸发现",
}


def source_name(source: str) -> str:
    """采集器名 → 中文显示名"""
    return SOURCE_NAMES.get(source, source)


@dataclass
class WhaleDetails:
    """巨鲸事件卡片的可选详情；所有文本均按不可信外部内容处理。"""

    address: str
    label: str | None = None
    tags: list[str] = field(default_factory=list)
    coin: str | None = None
    direction: str | None = None
    quantity: str | None = None
    amount_usd: str | None = None
    entry_px: str | None = None
    liquidation_px: str | None = None
    liquidation_distance: str | None = None
    configured_leverage: str | None = None
    actual_leverage: str | None = None
    high_leverage: bool = False
    pnl: str | None = None
    tier: str | None = None


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
    whale: WhaleDetails | None = None


@dataclass
class SendResult:
    """一次推送的结果

    Attributes:
        ok: 是否发送成功
        retryable: 失败时是否值得重试（网络错误、限流等为 True，参数或签名错误等为 False）
        detail: 结果说明，写入 push_log 便于排查
    """

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
