import asyncio
import hashlib
import hmac
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx
from websockets.asyncio.server import serve

from tidewatch.collectors.announcements import binance_dedupe_key
from tidewatch.collectors.binance_ann import (
    BINANCE_LIST_URL,
    BinanceAnnPoll,
    BinanceAnnStream,
    parse_article,
    parse_ws_message,
    signed_ws_url,
)

FIX = Path(__file__).parent / "fixtures" / "binance"
LIST48 = json.loads((FIX / "list_48.json").read_text(encoding="utf-8"))
LIST161 = json.loads((FIX / "list_161.json").read_text(encoding="utf-8"))
NOW = datetime.fromtimestamp(1790673300000 / 1000 + 60, tz=timezone.utc)
ANN = {"catalogId": 48, "catalogName": "New Cryptocurrency Listing", "publishDate": 1790673000000,
       "title": "Binance Will List Example (EXM)", "body": "<p>Binance will list EXM.</p>", "disclaimer": ""}


def _articles(payload):
    return payload["data"]["catalogs"][0]["articles"]


def test_parse_list_articles():
    hype = parse_article(next(a for a in _articles(LIST48) if a["title"].startswith("Binance Will List Hyperliquid")), 48)
    assert hype.event_type == "spot_listing" and hype.assets == ["HYPE"] and hype.raw["exchange"] == "binance"
    assert hype.url.startswith("https://www.binance.com/en/support/announcement/")
    perp = parse_article(_articles(LIST48)[0], 48)
    assert perp.event_type == "perp_listing"
    removal = parse_article(_articles(LIST161)[0], 161)
    assert removal.event_type == "delisting" and removal.assets == []
    assert removal.dedupe_key == binance_dedupe_key(removal.title, removal.occurred_at)


def test_ws_message_parsing_and_key_matches_list():
    message = json.dumps({"type": "DATA", "topic": "com_announcement_en", "data": json.dumps(ANN)})
    event = parse_ws_message(message)
    assert event.title == "Binance Will List Example (EXM)" and event.event_type == "spot_listing"
    assert event.assets == ["EXM"] and event.body == "Binance will list EXM."
    article = {"id": 1, "code": "abc", "title": "Binance Will List Example (EXM)", "releaseDate": 1790673000500}
    assert parse_article(article, 48).dedupe_key == event.dedupe_key  # WS 与列表接口键一致
    assert parse_ws_message(json.dumps({"type": "COMMAND", "data": "SUCCESS", "subType": "SUBSCRIBE"})) is None


def test_signed_ws_url():
    url = signed_ws_url("secret", 1790700000000, "abc", base_url="wss://example/sapi/wss")
    query = "random=abc&recvWindow=30000&timestamp=1790700000000&topic=com_announcement_en"
    expected = hmac.new(b"secret", query.encode(), hashlib.sha256).hexdigest()
    assert url == f"wss://example/sapi/wss?{query}&signature={expected}"


@respx.mock
async def test_poll_fetch_list_within_window(settings):
    respx.get(BINANCE_LIST_URL).mock(
        side_effect=lambda request: httpx.Response(200, json=LIST48 if request.url.params["catalogId"] == "48" else LIST161)
    )
    cfg = settings.collector("binance_ann").model_copy(update={"rate_per_s": 100})  # 测试中不等待限流
    collector = BinanceAnnPoll(cfg, sink=None, health=None,
                               client=httpx.AsyncClient(trust_env=False), engine=None, clock=lambda: NOW)
    result = await collector.fetch(None)
    assert len(result.events) == 3  # 161 两条 + 48 一条落在 6 小时补抓窗口内
    assert result.cursor == {"last_ms": 1790673300000}
    again = await collector.fetch(result.cursor)
    # 只按补抓窗口过滤：再次拉取仍返回窗口内条目，由入库去重保证只入库一次
    assert [e.dedupe_key for e in again.events] == [e.dedupe_key for e in result.events]
    assert again.cursor == {"last_ms": 1790673300000}


async def test_stream_with_local_server(settings):
    received = {}

    async def handler(ws):
        received["header"] = ws.request.headers.get("X-MBX-APIKEY")
        received["command"] = json.loads(await ws.recv())
        await ws.send(json.dumps({"type": "COMMAND", "data": "SUCCESS", "subType": "SUBSCRIBE", "code": "00000000"}))
        await ws.send(json.dumps({"type": "DATA", "topic": "com_announcement_en", "data": json.dumps(ANN)}))
        await ws.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        collector = BinanceAnnStream(settings.collector("binance_ann"), api_key="k", api_secret="s",
                                     ws_url=f"ws://127.0.0.1:{port}/sapi/wss", sink=None, health=None,
                                     client=None, engine=None, clock=lambda: NOW)
        stream = collector.stream(None)
        first = await asyncio.wait_for(stream.__anext__(), timeout=5)
        second = await asyncio.wait_for(stream.__anext__(), timeout=5)
        await stream.aclose()
    assert received["header"] == "k"
    assert received["command"] == {"command": "SUBSCRIBE", "value": "com_announcement_en"}
    assert first.events == [] and first.cursor is None  # 订阅确认按心跳处理
    assert second.events[0].title == ANN["title"] and second.cursor == {"last_ms": 1790673000000}
    assert collector.merge_cursor({"last_ms": 5}, {"last_ms": 3}) == {"last_ms": 5}


async def test_stream_raises_on_command_error(settings):
    """订阅/鉴权被拒绝（如 API Key 无效）时必须报错重连，不能当成心跳"""

    async def handler(ws):
        await ws.recv()
        await ws.send(json.dumps({"type": "COMMAND", "data": "Invalid Api-Key ID.", "subType": "REGISTER"}))
        # 处理函数返回后服务端关闭连接

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        collector = BinanceAnnStream(settings.collector("binance_ann"), api_key="bad", api_secret="s",
                                     ws_url=f"ws://127.0.0.1:{port}/sapi/wss", sink=None, health=None,
                                     client=None, engine=None, clock=lambda: NOW)
        stream = collector.stream(None)
        try:
            with pytest.raises(RuntimeError) as excinfo:
                await asyncio.wait_for(stream.__anext__(), timeout=5)
        finally:
            await stream.aclose()
    assert str(excinfo.value) == "币安公告 WS 返回错误：Invalid Api-Key ID.（REGISTER）"


def test_parse_ws_message_command_error_raises_but_success_is_ignored():
    error = json.dumps({"type": "COMMAND", "data": "Invalid Api-Key ID.", "subType": "REGISTER"})
    with pytest.raises(RuntimeError, match="Invalid Api-Key ID"):
        parse_ws_message(error)
    assert parse_ws_message(json.dumps({"type": "COMMAND", "data": "SUCCESS", "subType": "SUBSCRIBE"})) is None


def _list_payload(articles: list[dict]) -> dict:
    return {"code": "000000", "success": True, "data": {"catalogs": [{"articles": articles}]}}


def _article(code: str, title: str, release_ms: int) -> dict:
    return {"id": 1, "code": code, "title": title, "releaseDate": release_ms}


@respx.mock
async def test_fetch_list_events_sorted_by_time_then_dedupe_key(settings):
    t = 1790673000000
    listing = _list_payload([  # 接口按时间倒序返回
        _article("d", "Binance Will List DDD (DDD)", t + 4000),
        _article("a", "Binance Will List AAA (AAA)", t + 1000),
    ])
    delisting = _list_payload([
        _article("b", "Notice of Removal of Spot Trading Pairs - 2026-10-02", t + 3000),
        _article("c", "Binance Will Delist CCC (CCC)", t + 1000),
    ])
    respx.get(BINANCE_LIST_URL).mock(
        side_effect=lambda request: httpx.Response(200, json=listing if request.url.params["catalogId"] == "48" else delisting)
    )
    cfg = settings.collector("binance_ann").model_copy(update={"rate_per_s": 100})
    collector = BinanceAnnPoll(cfg, sink=None, health=None, client=httpx.AsyncClient(trust_env=False),
                               engine=None, clock=lambda: NOW)
    events = (await collector.fetch(None)).events
    assert [(e.occurred_at, e.dedupe_key) for e in events] == sorted((e.occurred_at, e.dedupe_key) for e in events)
    times = [int(e.occurred_at.timestamp() * 1000) for e in events]
    assert times == [t + 1000, t + 1000, t + 3000, t + 4000]


@respx.mock
@pytest.mark.parametrize("missing", ["releaseDate", "title"])
async def test_fetch_list_error_mentions_article_code(settings, missing):
    bad = _article("badcode123", "Binance Will List BAD (BAD)", 1790673000000)
    del bad[missing]
    respx.get(BINANCE_LIST_URL).mock(return_value=httpx.Response(200, json=_list_payload([bad])))
    cfg = settings.collector("binance_ann").model_copy(update={"rate_per_s": 100})
    collector = BinanceAnnPoll(cfg, sink=None, health=None, client=httpx.AsyncClient(trust_env=False),
                               engine=None, clock=lambda: NOW)
    with pytest.raises(RuntimeError, match="badcode123"):
        await collector.fetch(None)


def test_parse_ws_message_error_mentions_title():
    bad = {"catalogId": 48, "title": "Binance Will List Broken (BRK)", "body": ""}  # 缺少 publishDate
    with pytest.raises(RuntimeError, match="Binance Will List Broken"):
        parse_ws_message(json.dumps({"type": "DATA", "topic": "com_announcement_en", "data": json.dumps(bad)}))
