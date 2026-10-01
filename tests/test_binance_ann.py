import asyncio
import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx
from websockets.asyncio.server import serve

from fakes import FakeHealth, FakeNotifier, FakeSink, FakeSleep, wait_until
import tidewatch.collectors.binance_ann as binance_ann
from tidewatch.collectors.announcements import binance_dedupe_key
from tidewatch.collectors.binance_ann import (
    BINANCE_LIST_URL,
    WS_TOPIC,
    BinanceAnnPoll,
    BinanceAnnStream,
    parse_article,
    parse_ws_message,
    signed_ws_url,
)
from tidewatch.health import HealthMonitor
from tidewatch.models import FetchResult

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


def _page_articles(prefix: str, newest_ms: int, count: int = 20) -> list[dict]:
    """生成按发布时间倒序排列的一页公告。"""
    return [
        _article(f"{prefix}-{index}", f"Binance Will List {prefix}{index} ({prefix}{index})", newest_ms - index * 1_000)
        for index in range(count)
    ]


def _requested_pages(route, catalog_id: int) -> list[str]:
    return [
        call.request.url.params["pageNo"]
        for call in route.calls
        if call.request.url.params["catalogId"] == str(catalog_id)
    ]


@respx.mock
async def test_fetch_list_paginates_then_stops_after_reaching_cursor(settings):
    now_ms = int(NOW.timestamp() * 1_000)
    cursor = now_ms - 60_000
    pages = {
        (48, 1): _page_articles("P1", now_ms - 1_000),
        (48, 2): [*_page_articles("P2", now_ms - 21_000, 19), _article("cursor", "Binance Will List CUR (CUR)", cursor)],
        (48, 3): _page_articles("NEVER", now_ms - 70_000),
    }

    def respond(request: httpx.Request) -> httpx.Response:
        key = (int(request.url.params["catalogId"]), int(request.url.params["pageNo"]))
        return httpx.Response(200, json=_list_payload(pages.get(key, [])))

    route = respx.get(BINANCE_LIST_URL).mock(side_effect=respond)
    result = await _poll(settings).fetch({"last_ms": cursor})

    assert _requested_pages(route, 48) == ["1", "2"]
    assert _requested_pages(route, 161) == ["1"]
    assert all(call.request.url.params["pageSize"] == "20" for call in route.calls)
    assert len(result.events) == 40
    assert result.cursor == {"last_ms": now_ms - 1_000}


@respx.mock
async def test_fetch_list_page_limit_warns_and_advances_cursor(settings, monkeypatch, caplog):
    monkeypatch.setattr(binance_ann, "MAX_PAGES", 2)
    now_ms = int(NOW.timestamp() * 1_000)
    cursor = now_ms - 3_000_000
    pages = {
        (48, 1): _page_articles("P1", now_ms - 1_000),
        (48, 2): _page_articles("P2", now_ms - 21_000),
    }

    def respond(request: httpx.Request) -> httpx.Response:
        key = (int(request.url.params["catalogId"]), int(request.url.params["pageNo"]))
        return httpx.Response(200, json=_list_payload(pages.get(key, [])))

    route = respx.get(BINANCE_LIST_URL).mock(side_effect=respond)
    with caplog.at_level(logging.WARNING, logger="tidewatch.collectors.binance_ann"):
        result = await _poll(settings).fetch({"last_ms": cursor})

    assert _requested_pages(route, 48) == ["1", "2"]
    assert result.cursor == {"last_ms": now_ms - 1_000}
    assert any(
        record.levelno == logging.WARNING
        and "币安公告栏目 48" in record.getMessage()
        and "翻页达到上限 2 页" in record.getMessage()
        for record in caplog.records
    )


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


# ---------------------------------------------------------------------------
# 单条畸形 WS 消息：记 warning 并跳过，不断开连接
# ---------------------------------------------------------------------------

BAD_ANN_NO_DATE = {"catalogId": 48, "title": "Binance Will List Broken (BRK)", "body": ""}  # 缺少 publishDate
LONG_GARBAGE = "x" * 500


def _data_message(payload) -> str:
    return json.dumps({"type": "DATA", "topic": "com_announcement_en", "data": payload})


class _WsOnlyStream(BinanceAnnStream):
    """补缺与游标读取不走 REST / 数据库，只测 WS 路径"""

    async def load_cursor(self):
        return None

    async def gap_fill(self, cursor):
        return FetchResult([], None)


class _BackoffOnlySleep(FakeSleep):
    """定时补缺任务的等待（>= 600 秒）一直挂起，只有退避等待才计数并在第 stop_after 次结束循环"""

    async def __call__(self, seconds: float) -> None:
        if seconds >= 600:
            await asyncio.Event().wait()
        await super().__call__(seconds)


def _ws_collector(settings, port: int, *, sink, health, sleep, cls=_WsOnlyStream) -> BinanceAnnStream:
    return cls(settings.collector("binance_ann"), api_key="k", api_secret="s",
               ws_url=f"ws://127.0.0.1:{port}/sapi/wss", sink=sink, health=health,
               client=None, engine=None, clock=lambda: NOW, sleep=sleep)


async def test_run_skips_malformed_ws_messages_and_still_ingests_next_event(settings, caplog):
    async def handler(ws):
        await ws.recv()
        await ws.send("not json at all")  # 非 JSON
        await ws.send(json.dumps([1, 2, 3]))  # 非 dict
        await ws.send(_data_message(json.dumps(BAD_ANN_NO_DATE)))  # DATA 缺字段
        await ws.send(_data_message("not-json-payload"))  # DATA 载荷不是 JSON
        await ws.send(_data_message(json.dumps([1, 2])))  # DATA 载荷不是对象
        await ws.send(LONG_GARBAGE)  # 很长的垃圾消息：日志只带前 200 字
        await ws.send(_data_message(json.dumps(ANN)))  # 之后的正常数据
        await ws.wait_closed()

    sink, health = FakeSink(), FakeHealth()
    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        collector = _ws_collector(settings, port, sink=sink, health=health, sleep=_BackoffOnlySleep(stop_after=1))
        task = asyncio.ensure_future(collector.run())
        try:
            with caplog.at_level(logging.WARNING, logger="tidewatch.collectors.binance_ann"):
                await wait_until(lambda: any(result.events for _, result in sink.calls), timeout=5)
        finally:
            task.cancel()
            await asyncio.wait({task}, timeout=5)
    ingested = [event for _, result in sink.calls for event in result.events]
    assert [event.title for event in ingested] == [ANN["title"]]  # 畸形消息之后的事件仍然入库
    assert health.failures == [] and health.successes  # 连接没有被断开，事件入库成功后上报成功
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 6
    assert "not json at all" in warnings[0] and "已跳过" in warnings[0]
    assert "Binance Will List Broken" in warnings[2]  # 缺字段那条：带标题上下文
    assert "x" * 200 in warnings[5] and "x" * 201 not in warnings[5]  # 只带前 200 字


async def test_run_command_error_after_malformed_message_still_fails_connection(settings):
    async def handler(ws):
        await ws.recv()
        await ws.send("not json at all")
        await ws.send(json.dumps({"type": "COMMAND", "data": "Invalid Api-Key ID.", "subType": "REGISTER"}))
        await ws.wait_closed()

    health = FakeHealth()
    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        collector = _ws_collector(settings, port, sink=FakeSink(), health=health, sleep=_BackoffOnlySleep(stop_after=1))
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(collector.run(), timeout=5)
    assert len(health.failures) == 1 and "Invalid Api-Key ID." in health.failures[0][1]


@pytest.mark.parametrize(
    "raw",
    [
        "not json at all",
        "[1, 2, 3]",
        '"just a string"',
        "123",
        _data_message("not-json-payload"),
        _data_message(json.dumps([1, 2])),
    ],
)
def test_parse_ws_message_malformed_raises_value_error(raw):
    with pytest.raises(ValueError):
        parse_ws_message(raw)


# ---------------------------------------------------------------------------
# 订阅确认后立即断开的震荡：只有失败，没有成功
# ---------------------------------------------------------------------------


async def _flapping_handler(ws):
    await ws.recv()
    await ws.send(json.dumps({"type": "COMMAND", "data": "SUCCESS", "subType": "SUBSCRIBE", "code": "00000000"}))
    # 处理函数返回：服务端立即关闭连接


async def test_flapping_ws_connection_reports_only_failures(settings):
    health, sleep = FakeHealth(), _BackoffOnlySleep(stop_after=5)
    async with serve(_flapping_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        collector = _ws_collector(settings, port, sink=FakeSink(), health=health, sleep=sleep)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(collector.run(), timeout=10)
    assert health.successes == []  # 订阅确认不是成功：否则每轮成功都会清零连续失败
    assert len(health.failures) == 5
    assert sleep.delays == [1, 2, 4, 8, 16]  # 退避也没有被重置


async def test_flapping_ws_connection_turns_real_health_abnormal(settings, engine, clock):
    notifier = FakeNotifier()
    monitor = HealthMonitor(engine, notifier, settings.dispatch, clock=clock)
    monitor.register("binance_ann", stale_after_s=180)
    async with serve(_flapping_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        collector = _ws_collector(settings, port, sink=FakeSink(), health=monitor, sleep=_BackoffOnlySleep(stop_after=5))
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(collector.run(), timeout=10)
    assert monitor.status("binance_ann") == "abnormal"
    assert [(source, ok) for source, ok, _ in notifier.health] == [("binance_ann", False)]


# ---------------------------------------------------------------------------
# 连接建立日志
# ---------------------------------------------------------------------------


async def test_stream_logs_connection_established_without_leaking_credentials(settings, caplog):
    async def handler(ws):
        await ws.recv()
        await ws.send(json.dumps({"type": "COMMAND", "data": "SUCCESS", "subType": "SUBSCRIBE"}))
        await ws.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        collector = _ws_collector(settings, port, sink=None, health=None, sleep=FakeSleep(stop_after=1))
        stream = collector.stream(None)
        with caplog.at_level(logging.INFO, logger="tidewatch.collectors.binance_ann"):
            await asyncio.wait_for(stream.__anext__(), timeout=5)
            await stream.aclose()
    infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any("已建立" in message and WS_TOPIC in message for message in infos)
    assert all("signature" not in message and "ws://" not in message for message in infos)  # 不记录带签名的地址


# ---------------------------------------------------------------------------
# 列表接口：success 为 false、catalogs 为空
# ---------------------------------------------------------------------------


def _poll(settings) -> BinanceAnnPoll:
    cfg = settings.collector("binance_ann").model_copy(update={"rate_per_s": 100})
    return BinanceAnnPoll(cfg, sink=None, health=None, client=httpx.AsyncClient(trust_env=False),
                          engine=None, clock=lambda: NOW)


@respx.mock
async def test_fetch_list_raises_when_success_is_false(settings):
    payload = {"code": "100001", "success": False, "message": "系统繁忙", "data": None}
    respx.get(BINANCE_LIST_URL).mock(return_value=httpx.Response(200, json=payload))
    with pytest.raises(RuntimeError, match="100001.*系统繁忙"):
        await _poll(settings).fetch(None)


@respx.mock
@pytest.mark.parametrize(
    "payload",
    [
        {"code": "000000", "success": True, "data": {"catalogs": []}},
        {"code": "000000", "success": True, "data": {}},
        {"code": "000000", "success": True, "data": None},
        {"code": "000000", "success": True, "data": {"catalogs": [{"articles": None}]}},
        {"code": "000000", "success": True, "data": {"catalogs": [{}]}},
    ],
)
async def test_fetch_list_with_no_articles_returns_empty_result(settings, payload):
    respx.get(BINANCE_LIST_URL).mock(return_value=httpx.Response(200, json=payload))
    collector = _poll(settings)
    first = await collector.fetch(None)
    assert first.events == [] and first.cursor is None
    again = await collector.fetch({"last_ms": 5})  # 有游标时保持原游标，不后退
    assert again.events == [] and again.cursor == {"last_ms": 5}
