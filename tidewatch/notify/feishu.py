"""飞书群自定义机器人通知器

安全约定：新闻标题、正文、公告标题、健康告警 detail 都是外部不可信文本，
一律使用 plain_text 展示，绝不放进 lark_md（否则会被解析为 @所有人、钓鱼链接等）。
lark_md 中只允许出现固定字段（时间、来源中文名、来源数量、资产代码），
其中资产代码来自配置或公告标题提取，写入前经 _md_safe 去除 Markdown 特殊字符。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
import unicodedata
from typing import Any, Callable

import httpx

from tidewatch.clock import format_local
from tidewatch.notify.base import EventCard, Notifier, SendResult, source_name

# 频率限制等可重试的业务错误码
RETRYABLE_CODES = {11232}
_COLORS = {"S": "red", "A": "orange"}

# 各文本字段的最大长度，超出时截断并加省略号
_TITLE_LIMIT = 80
_BODY_LIMIT = 300
_DIGEST_TITLE_LIMIT = 60
_DETAIL_LIMIT = 500

# lark_md 的特殊字符 → 全角替换字符（换行压成空格），避免被解析为标签、链接或强调
_MD_SAFE_TABLE = str.maketrans(
    {
        "<": "＜",
        ">": "＞",
        "[": "［",
        "]": "］",
        "(": "（",
        ")": "）",
        "*": "＊",
        "_": "＿",
        "~": "～",
        "`": "｀",
        "\n": " ",
        "\r": " ",
    }
)


def sign(timestamp: str, secret: str) -> str:
    """飞书签名：以 f"{timestamp}\\n{secret}" 为密钥对空消息做 HMAC-SHA256，再 Base64"""
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def _md_safe(text: str) -> str:
    """把文本中的 Markdown 特殊字符替换为全角字符，使其可安全放入 lark_md

    仅用于配置或公告标题提取出的短文本（如资产代码）；新闻标题、正文等
    自由文本不走这里，而是直接使用 plain_text。
    """
    return text.translate(_MD_SAFE_TABLE)


def _truncate(text: str, limit: int) -> str:
    """超过 limit 个字符时截断并追加「…」，否则原样返回"""
    return text if len(text) <= limit else text[:limit] + "…"


def _plain_div(content: str) -> dict[str, Any]:
    """构造一个 plain_text 文本块（内容不会被解析为 Markdown、@ 或链接）"""
    return {"tag": "div", "text": {"tag": "plain_text", "content": content}}


def _button_url(url: str | None) -> str | None:
    """返回可用作按钮链接的地址；仅 http:// 或 https:// 开头才有效（拒绝 javascript: 等协议）"""
    cleaned = (url or "").strip()
    return cleaned if cleaned.lower().startswith(("http://", "https://")) else None


def _body_text(title: str, body: str) -> str:
    """生成正文展示文本：去除与标题重复的开头并截断，无有效内容时返回空串"""
    title = title.strip()
    body = body.strip()
    if title and body.startswith(title):
        rest = body[len(title):]
        start = 0
        # 去掉标题后紧跟的空白与标点（如「：」「，」「。」）
        while start < len(rest) and (rest[start].isspace() or unicodedata.category(rest[start]).startswith("P")):
            start += 1
        body = rest[start:]
    return _truncate(body, _BODY_LIMIT) if body else ""


class FeishuNotifier(Notifier):
    """飞书群自定义机器人通知器：把事件、汇总、健康告警渲染为交互卡片并 POST 到 webhook

    - 配置了 secret 时按飞书规则签名；
    - 不可信文本只用 plain_text 展示，避免 @所有人 与链接注入；
    - 传入的 httpx 客户端由调用方负责关闭，自己创建的客户端由 aclose() 关闭。
    """

    def __init__(
        self,
        webhook: str,
        secret: str | None,
        *,
        client: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """初始化通知器

        Args:
            webhook: 飞书自定义机器人 webhook 地址
            secret: 签名密钥；为空表示机器人未开启签名校验
            client: 可选的外部 HTTP 客户端（测试注入），其生命周期由调用方管理
            clock: 返回当前 Unix 时间戳（秒）的函数，用于签名，便于测试注入
        """
        self._webhook = webhook
        self._secret = secret
        # 只关闭自己创建的客户端，避免误关调用方共享的连接池
        self._owns_client = client is None
        # 飞书在国内可直连，不使用环境代理
        self._client = client if client is not None else httpx.AsyncClient(timeout=10, trust_env=False)
        self._clock = clock

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        """发送单条事件卡片；mention_all 为 True 时在卡片中 @所有人"""
        return await self._post(self._event_card(card, mention_all))

    async def send_digest(self, cards: list[EventCard]) -> SendResult:
        """发送 A 级汇总卡片"""
        return await self._post(self._digest_card(cards))

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        """发送采集器健康告警（异常）或恢复通知"""
        return await self._post(self._health_card(source, healthy, detail))

    async def aclose(self) -> None:
        """关闭自己创建的 HTTP 客户端；注入的客户端不关闭"""
        if self._owns_client:
            await self._client.aclose()

    # ---------- 卡片 ----------

    @staticmethod
    def _event_card(card: EventCard, mention_all: bool) -> dict[str, Any]:
        """构造事件卡片

        结构：header（plain_text 标题）→ lark_md 固定字段块（@所有人、时间、来源、资产）
        → plain_text 正文块（可选）→ 「查看原文」按钮（仅 http/https 链接）。
        """
        lines = ["<at id=all></at>"] if mention_all else []
        lines.append(f"**时间**：{format_local(card.occurred_at)}")
        extra = f"（另有 {card.source_count - 1} 个来源）" if card.source_count > 1 else ""
        lines.append(f"**来源**：{_md_safe(source_name(card.source))}{extra}")
        if card.assets:
            lines.append(f"**资产**：{'、'.join(_md_safe(asset) for asset in card.assets)}")
        elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}]
        body = _body_text(card.title, card.body)
        if body:
            elements.append(_plain_div(body))
        url = _button_url(card.url)
        if url:
            elements.append(
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "查看原文"},
                            "type": "default",
                            "url": url,
                        }
                    ],
                }
            )
        prefix = "【升级】" if card.upgraded else ""
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": _COLORS.get(card.level, "blue"),
                "title": {"tag": "plain_text", "content": f"{prefix}【{card.level}】{_truncate(card.title, _TITLE_LIMIT)}"},
            },
            "elements": elements,
        }

    @staticmethod
    def _digest_card(cards: list[EventCard]) -> dict[str, Any]:
        """构造 A 级汇总卡片：整张汇总放在一个 plain_text 块中，每行「HH:MM [来源] 标题」

        标题是外部文本，不使用 Markdown，也不再拼接原文链接。
        """
        lines = []
        for card in cards:
            title = _truncate(" ".join(card.title.split()), _DIGEST_TITLE_LIMIT)  # 折叠换行，保证一条一行
            lines.append(f"{format_local(card.occurred_at, '%H:%M')} [{source_name(card.source)}] {title}")
        return {
            "config": {"wide_screen_mode": True},
            "header": {"template": "orange", "title": {"tag": "plain_text", "content": f"📋 A 级汇总（{len(cards)} 条）"}},
            "elements": [_plain_div("\n".join(lines))],
        }

    @staticmethod
    def _health_card(source: str, healthy: bool, detail: str) -> dict[str, Any]:
        """构造健康卡片：异常为红色、恢复为绿色；detail 可能含外部错误文本，使用 plain_text"""
        title = f"✅ 采集器恢复：{source_name(source)}" if healthy else f"⚠️ 采集器异常：{source_name(source)}"
        return {
            "config": {"wide_screen_mode": True},
            "header": {"template": "green" if healthy else "red", "title": {"tag": "plain_text", "content": title}},
            "elements": [_plain_div(_truncate(detail, _DETAIL_LIMIT))],
        }

    # ---------- 发送 ----------

    def _payload(self, card: dict[str, Any]) -> dict[str, Any]:
        """组装请求体；配置了 secret 时附带 timestamp 与 sign"""
        payload: dict[str, Any] = {"msg_type": "interactive", "card": card}
        if self._secret:
            timestamp = str(int(self._clock()))
            payload["timestamp"] = timestamp
            payload["sign"] = sign(timestamp, self._secret)
        return payload

    async def _post(self, card: dict[str, Any]) -> SendResult:
        """POST 卡片并把响应归类为 SendResult

        - 网络错误、HTTP 429/5xx、频率限制业务码：可重试；
        - 非 JSON、非对象 JSON、其他业务错误码：不可重试；
        - 任何响应形态都不抛异常。
        """
        try:
            response = await self._client.post(self._webhook, json=self._payload(card))
        except httpx.HTTPError as exc:
            return SendResult(False, retryable=True, detail=f"网络错误：{type(exc).__name__} {exc}")
        if response.status_code == 429 or response.status_code >= 500:
            return SendResult(False, retryable=True, detail=f"HTTP {response.status_code}")
        try:
            data = response.json()
        except ValueError:
            return SendResult(False, retryable=False, detail=f"响应不是 JSON：HTTP {response.status_code}")
        if not isinstance(data, dict):
            return SendResult(
                False,
                retryable=False,
                detail=f"响应格式异常：{type(data).__name__} {str(data)[:100]}（HTTP {response.status_code}）",
            )
        code = data.get("code", data.get("StatusCode", -1))
        if code == 0:
            return SendResult(True, detail="code=0")
        # code 可能是不可哈希的类型（如列表），先判断类型再查集合
        retryable = isinstance(code, int) and code in RETRYABLE_CODES
        return SendResult(False, retryable=retryable, detail=f"code={code} msg={data.get('msg')}")
