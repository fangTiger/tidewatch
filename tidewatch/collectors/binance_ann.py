"""币安公告采集器

- BinanceAnnStream：有只读 API Key 时订阅公告 WS，并用列表接口在（重）连接后及每 10 分钟补缺
- BinanceAnnPoll：没有 API Key 时的降级模式，定时轮询列表接口
两种来源使用同一个去重键（规范化标题 + 发布日期），同一公告只入库一次。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator
from urllib.parse import urlencode

from websockets.asyncio.client import connect

from tidewatch.collectors.announcements import binance_dedupe_key, classify, extract_symbols
from tidewatch.collectors.base import PollCollector, StreamCollector, describe, parse_context, sort_events
from tidewatch.config import CollectorCfg
from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.normalize import clean_text

BINANCE_LIST_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
BINANCE_WS_URL = "wss://api.binance.com/sapi/wss"
WS_TOPIC = "com_announcement_en"
# 列表接口轮询的栏目及其分类大类
LIST_CATALOGS = {48: "listing", 161: "delisting"}
PAGE_SIZE = 20
MAX_PAGES = 5
WS_RECV_TIMEOUT_S = 60
# 日志里最多带多少个字符的原始消息
_LOG_PREVIEW_CHARS = 200

logger = logging.getLogger(__name__)


class BinanceCommandError(RuntimeError):
    """WS 的 COMMAND 消息返回了非 SUCCESS 的结果（如 API Key 无效）：连接级错误，必须断开重连，不能当作畸形消息跳过"""


def _published(ms: int | str) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def _make_event(
    title: str, published: datetime, catalog_id: int | None, *, url: str | None, body: str = "", extra: dict | None = None
) -> Event:
    title = title.strip()
    return Event(
        source="binance_ann",
        category="exchange",
        event_type=classify(title, LIST_CATALOGS.get(catalog_id)),
        occurred_at=published,
        title=title,
        body=body,
        url=url,
        dedupe_key=binance_dedupe_key(title, published),
        assets=extract_symbols(title),
        raw={"exchange": "binance", "catalogId": catalog_id, **(extra or {})},
    )


def _article_label(article: dict[str, Any]) -> str:
    return f"币安公告 {article.get('code') or article.get('id') or article.get('title')}"


def _release_ms(article: dict[str, Any]) -> int:
    with parse_context(_article_label(article)):
        return int(article["releaseDate"])


def parse_article(article: dict[str, Any], catalog_id: int) -> Event:
    """列表接口的一篇公告 → 事件；解析失败时异常消息带上公告 code"""
    with parse_context(_article_label(article)):
        return _make_event(
            article["title"],
            _published(article["releaseDate"]),
            catalog_id,
            url=f"https://www.binance.com/en/support/announcement/{article['code']}",
            extra={"code": article.get("code")},
        )


def parse_ws_message(raw: str | bytes) -> Event | None:
    """WS 消息 → 事件

    订阅确认（COMMAND/SUCCESS）等非数据消息返回 None；
    命令被拒绝（COMMAND 且结果不是 SUCCESS，如 API Key 无效）抛 BinanceCommandError（RuntimeError 的子类），
    让采集器报失败并重连；
    单条消息本身畸形（非 JSON、不是 JSON 对象、DATA 缺字段）抛 ValueError / RuntimeError，
    由调用方决定跳过，不影响连接。
    """
    message = json.loads(raw)
    if not isinstance(message, dict):
        raise ValueError(f"消息不是 JSON 对象：{type(message).__name__}")
    if message.get("type") == "COMMAND" and message.get("data") != "SUCCESS":
        raise BinanceCommandError(f"币安公告 WS 返回错误：{message.get('data')}（{message.get('subType')}）")
    if message.get("type") != "DATA":
        return None
    data = message.get("data")
    if isinstance(data, str):
        data = json.loads(data)
    if not data:
        return None
    if not isinstance(data, dict):
        raise ValueError(f"DATA 载荷不是 JSON 对象：{type(data).__name__}")
    if not data.get("title"):
        return None
    catalog_id = data.get("catalogId")
    with parse_context(f"币安公告 WS 消息 {data['title']}"):
        return _make_event(
            data["title"],
            _published(data["publishDate"]),
            catalog_id,
            url=f"https://www.binance.com/en/support/announcement/list/{catalog_id}" if catalog_id else None,
            body=clean_text(data.get("body")),
            extra={"catalogName": data.get("catalogName")},
        )


def _preview(raw: str | bytes) -> str:
    text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
    return text[:_LOG_PREVIEW_CHARS]


def signed_ws_url(api_secret: str, now_ms: int, nonce: str, *, base_url: str = BINANCE_WS_URL) -> str:
    """生成带签名的 WS 地址：对按键名排序的查询串做 HMAC-SHA256"""
    params = {"random": nonce, "recvWindow": 30000, "timestamp": now_ms, "topic": WS_TOPIC}
    query = urlencode(sorted(params.items()))
    signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{base_url}?{query}&signature={signature}"


class _BinanceList:
    """列表接口：各栏目按边界翻页补抓（混入类，依赖采集器基类的 client、limiter、clock、cfg）"""

    async def _list_page(self, catalog_id: int, page_no: int) -> list[dict[str, Any]]:
        await self.limiter.acquire()
        response = await self.client.get(
            BINANCE_LIST_URL,
            params={"type": 1, "catalogId": catalog_id, "pageNo": page_no, "pageSize": PAGE_SIZE},
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(f"币安公告接口返回错误：{payload.get('code')} {payload.get('message')}")
        catalogs = (payload.get("data") or {}).get("catalogs") or []
        return (catalogs[0].get("articles") if catalogs else None) or []

    async def fetch_list(self, cursor: dict[str, Any] | None) -> FetchResult:
        last = int((cursor or {}).get("last_ms", 0))
        floor_ms = int((self.clock() - timedelta(seconds=self.cfg.backfill_max_s)).timestamp() * 1000)
        boundary_ms = max(last, floor_ms)
        events: list[Event] = []
        newest = last
        for catalog_id in LIST_CATALOGS:
            for page_no in range(1, MAX_PAGES + 1):
                articles = await self._list_page(catalog_id, page_no)
                published = [_release_ms(article) for article in articles]
                for article, published_ms in zip(articles, published):
                    newest = max(newest, published_ms)
                    # 只按补抓窗口过滤、不按游标过滤：两个栏目共用一个游标，若按游标过滤，
                    # 某栏目延迟出现的较早公告会被永久跳过；重复条目由去重键处理
                    if published_ms >= floor_ms:
                        events.append(parse_article(article, catalog_id))
                if len(articles) < PAGE_SIZE or any(published_ms <= boundary_ms for published_ms in published):
                    break
            else:
                logger.warning(
                    "币安公告栏目 %d 翻页达到上限 %d 页仍未追上游标或补抓窗口，可能有更早的公告未采集",
                    catalog_id,
                    MAX_PAGES,
                )
        return FetchResult(sort_events(events), {"last_ms": newest} if newest else None)


class BinanceAnnPoll(_BinanceList, PollCollector):
    """降级模式：没有 API Key 时轮询列表接口"""

    name = "binance_ann"
    group = "news"

    async def fetch(self, cursor: dict[str, Any] | None) -> FetchResult:
        return await self.fetch_list(cursor)


class BinanceAnnStream(_BinanceList, StreamCollector):
    """WS 模式：订阅公告推送，列表接口补缺"""

    name = "binance_ann"
    group = "news"

    def __init__(
        self,
        cfg: CollectorCfg,
        *,
        api_key: str,
        api_secret: str,
        ws_proxy: str | None = None,
        ws_url: str = BINANCE_WS_URL,
        **kwargs: Any,
    ) -> None:
        super().__init__(cfg, **kwargs)
        self._api_key = api_key
        self._api_secret = api_secret
        self._ws_proxy = ws_proxy
        self._ws_url = ws_url

    async def gap_fill(self, cursor: dict[str, Any] | None) -> FetchResult:
        return await self.fetch_list(cursor)

    def merge_cursor(self, old: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any] | None:
        if new is None:
            return old
        return {"last_ms": max(int((old or {}).get("last_ms", 0)), int(new["last_ms"]))}

    async def stream(self, cursor: dict[str, Any] | None) -> AsyncIterator[FetchResult]:
        url = signed_ws_url(self._api_secret, int(self.clock().timestamp() * 1000), uuid.uuid4().hex, base_url=self._ws_url)
        async with connect(
            url,
            additional_headers={"X-MBX-APIKEY": self._api_key},
            proxy=self._ws_proxy,
            ping_interval=30,
            ping_timeout=20,
            open_timeout=20,
        ) as ws:
            await ws.send(json.dumps({"command": "SUBSCRIBE", "value": WS_TOPIC}))
            logger.info("币安公告 WS 连接已建立并发送订阅（topic=%s）", WS_TOPIC)  # 不记录带签名的地址
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT_S)
                except asyncio.TimeoutError:
                    # 一段时间没有公告：连接仍由 websockets 自动 ping 保活，按心跳处理
                    yield FetchResult([], None)
                    continue
                try:
                    event = parse_ws_message(raw)
                except BinanceCommandError:
                    raise  # 订阅/鉴权被拒绝：必须断开重连
                except Exception as exc:
                    # 单条畸形消息（非 JSON、字段缺失等）不值得断开连接：记录原文开头后跳过
                    logger.warning(
                        "币安公告 WS 收到无法解析的消息，已跳过：%s；消息前 %d 字：%s",
                        describe(exc), _LOG_PREVIEW_CHARS, _preview(raw),
                    )
                    continue
                if event is None:
                    yield FetchResult([], None)
                else:
                    yield FetchResult([event], {"last_ms": int(event.occurred_at.timestamp() * 1000)})
