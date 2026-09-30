"""飞书群自定义机器人通知器"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any, Callable

import httpx

from tidewatch.clock import format_local
from tidewatch.notify.base import EventCard, Notifier, SendResult, source_name

# 频率限制等可重试的业务错误码
RETRYABLE_CODES = {11232}
_COLORS = {"S": "red", "A": "orange"}


def sign(timestamp: str, secret: str) -> str:
    """飞书签名：以 f"{timestamp}\\n{secret}" 为密钥对空消息做 HMAC-SHA256，再 Base64"""
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


class FeishuNotifier(Notifier):
    def __init__(
        self,
        webhook: str,
        secret: str | None,
        *,
        client: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._webhook = webhook
        self._secret = secret
        # 飞书在国内可直连，不使用环境代理
        self._client = client or httpx.AsyncClient(timeout=10, trust_env=False)
        self._clock = clock

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        return await self._post(self._event_card(card, mention_all))

    async def send_digest(self, cards: list[EventCard]) -> SendResult:
        return await self._post(self._digest_card(cards))

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        return await self._post(self._health_card(source, healthy, detail))

    async def aclose(self) -> None:
        await self._client.aclose()

    # ---------- 卡片 ----------

    @staticmethod
    def _event_card(card: EventCard, mention_all: bool) -> dict[str, Any]:
        lines = ["<at id=all></at>"] if mention_all else []
        lines.append(f"**时间**：{format_local(card.occurred_at)}")
        extra = f"（另有 {card.source_count - 1} 个来源）" if card.source_count > 1 else ""
        lines.append(f"**来源**：{source_name(card.source)}{extra}")
        if card.assets:
            lines.append(f"**资产**：{'、'.join(card.assets)}")
        if card.body and card.body != card.title:
            lines.append(card.body[:300])
        elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}]
        if card.url:
            elements.append(
                {
                    "tag": "action",
                    "actions": [
                        {"tag": "button", "text": {"tag": "plain_text", "content": "查看原文"}, "type": "default", "url": card.url}
                    ],
                }
            )
        prefix = "【升级】" if card.upgraded else ""
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": _COLORS.get(card.level, "blue"),
                "title": {"tag": "plain_text", "content": f"{prefix}【{card.level}】{card.title[:80]}"},
            },
            "elements": elements,
        }

    @staticmethod
    def _digest_card(cards: list[EventCard]) -> dict[str, Any]:
        lines = []
        for card in cards:
            link = f" [原文]({card.url})" if card.url else ""
            lines.append(f"- {format_local(card.occurred_at, '%H:%M')} [{source_name(card.source)}] {card.title[:60]}{link}")
        return {
            "config": {"wide_screen_mode": True},
            "header": {"template": "orange", "title": {"tag": "plain_text", "content": f"📋 A 级汇总（{len(cards)} 条）"}},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}],
        }

    @staticmethod
    def _health_card(source: str, healthy: bool, detail: str) -> dict[str, Any]:
        title = f"✅ 采集器恢复：{source_name(source)}" if healthy else f"⚠️ 采集器异常：{source_name(source)}"
        return {
            "config": {"wide_screen_mode": True},
            "header": {"template": "green" if healthy else "red", "title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": detail[:500]}}],
        }

    # ---------- 发送 ----------

    def _payload(self, card: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {"msg_type": "interactive", "card": card}
        if self._secret:
            timestamp = str(int(self._clock()))
            payload["timestamp"] = timestamp
            payload["sign"] = sign(timestamp, self._secret)
        return payload

    async def _post(self, card: dict[str, Any]) -> SendResult:
        try:
            response = await self._client.post(self._webhook, json=self._payload(card))
        except httpx.HTTPError as exc:
            return SendResult(False, retryable=True, detail=f"网络错误：{exc}")
        if response.status_code == 429 or response.status_code >= 500:
            return SendResult(False, retryable=True, detail=f"HTTP {response.status_code}")
        try:
            data = response.json()
        except ValueError:
            return SendResult(False, retryable=False, detail=f"响应不是 JSON：HTTP {response.status_code}")
        code = data.get("code", data.get("StatusCode", -1))
        if code == 0:
            return SendResult(True, detail="code=0")
        return SendResult(False, retryable=code in RETRYABLE_CODES, detail=f"code={code} msg={data.get('msg')}")
