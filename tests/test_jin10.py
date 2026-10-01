import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from tidewatch.collectors import jin10
from tidewatch.collectors.jin10 import JIN10_URL, Jin10Collector, parse_item

PAGE1 = json.loads((Path(__file__).parent / "fixtures" / "jin10" / "flash_page1.json").read_text(encoding="utf-8"))
NOW = datetime(2026, 9, 30, 3, 1, tzinfo=timezone.utc)  # 上海时间 11:01


def _collector(settings) -> Jin10Collector:
    return Jin10Collector(settings.collector("jin10"), sink=None, health=None, client=httpx.AsyncClient(trust_env=False),
                          engine=None, clock=lambda: NOW)


def test_parse_items_from_fixture():
    items = PAGE1["data"]
    locked = parse_item(items[0])
    assert locked.title == "瑞典北欧斯安银行：油市正被时间、地域和品种撕裂，现货与期货价格严重分化"
    assert locked.body == "" and locked.raw["locked"] is True
    assert locked.occurred_at == datetime(2026, 9, 30, 3, 0, 28, tzinfo=timezone.utc)
    article = parse_item(next(i for i in items if i["id"] == "20260930105126319800"))
    assert article.event_type == "article" and article.raw["important"] == 1
    assert article.url == "https://xnews.jin10.com/details/231352"
    flash = parse_item(next(i for i in items if i["id"] == "20260930105037539800"))
    assert flash.event_type == "flash" and flash.title.startswith("港股低开高走")
    assert flash.url == "https://flash.jin10.com/detail/20260930105037539800"
    assert flash.dedupe_key == "jin10:20260930105037539800"
    assert sum(parse_item(i).raw["important"] for i in items) == 6
    assert parse_item({"id": "1", "time": "2026-09-30 10:00:00", "type": 0, "data": {"content": ""}}) is None


@respx.mock
async def test_first_run_takes_first_page(settings):
    route = respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    result = await _collector(settings).fetch(None)
    assert len(result.events) == 20 and route.call_count == 1
    assert result.cursor == {"last_id": "20260930110028586800"}
    assert route.calls.last.request.headers["x-app-id"] == "bVBF4FyRTn5NJF5n"


@respx.mock
async def test_incremental_stops_at_cursor(settings):
    route = respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    result = await _collector(settings).fetch({"last_id": "20260930105137078800"})
    assert len(result.events) == 4 and route.call_count == 1


@respx.mock
async def test_paginates_until_cursor(settings):
    older = {"status": 200, "data": [
        {"id": "20260930103000000000", "time": "2026-09-30 10:30:00", "type": 0, "important": 0,
         "data": {"content": "更早的一条快讯"}},
        {"id": "20260930100000000000", "time": "2026-09-30 10:00:00", "type": 0, "important": 0,
         "data": {"content": "已经采集过的快讯"}},
    ]}
    route = respx.get(JIN10_URL).mock(side_effect=[httpx.Response(200, json=PAGE1), httpx.Response(200, json=older)])
    result = await _collector(settings).fetch({"last_id": "20260930100000000000"})
    assert len(result.events) == 21 and route.call_count == 2
    assert route.calls[1].request.url.params["max_time"] == "2026-09-30 10:36:50"
    assert result.cursor == {"last_id": "20260930110028586800"}


@respx.mock
async def test_bad_status_raises(settings):
    respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json={"status": 500, "message": "错误", "data": []}))
    with pytest.raises(RuntimeError):
        await _collector(settings).fetch(None)


def _item(item_id: str, time: str) -> dict:
    return {"id": item_id, "time": time, "type": 0, "important": 0, "data": {"content": f"快讯{item_id}"}}


@respx.mock
async def test_events_sorted_by_time_then_dedupe_key(settings):
    page = {"status": 200, "data": [  # 接口按时间倒序返回，同一秒的条目 id 乱序
        _item("20260930104000000003", "2026-09-30 10:40:00"),
        _item("20260930104000000001", "2026-09-30 10:40:00"),
        _item("20260930103000000000", "2026-09-30 10:30:00"),
        _item("20260930104000000002", "2026-09-30 10:40:00"),
    ]}
    respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json=page))
    result = await _collector(settings).fetch(None)
    assert [e.dedupe_key for e in result.events] == [
        "jin10:20260930103000000000", "jin10:20260930104000000001",
        "jin10:20260930104000000002", "jin10:20260930104000000003",
    ]


@respx.mock
async def test_fixture_events_sorted_ascending(settings):
    respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    events = (await _collector(settings).fetch(None)).events
    times = [e.occurred_at for e in events]
    assert times == sorted(times) and times[0] < times[-1]


@respx.mock
async def test_overlapping_pages_do_not_duplicate_events(settings):
    # 翻页时 max_time 多取 1 秒，第二页会与第一页重叠（这里重叠第一页最后一条）
    overlap = PAGE1["data"][-1]
    page2 = {"status": 200, "data": [
        overlap,
        _item("20260930103000000000", "2026-09-30 10:30:00"),
        _item("20260930100000000000", "2026-09-30 10:00:00"),  # 游标
    ]}
    route = respx.get(JIN10_URL).mock(side_effect=[httpx.Response(200, json=PAGE1), httpx.Response(200, json=page2)])
    result = await _collector(settings).fetch({"last_id": "20260930100000000000"})
    keys = [e.dedupe_key for e in result.events]
    assert route.call_count == 2
    assert len(keys) == len(set(keys)) == 21


@respx.mock
async def test_page_limit_logs_warning(settings, monkeypatch, caplog):
    monkeypatch.setattr(jin10, "MAX_PAGES", 2)
    page2 = {"status": 200, "data": [_item("20260930103000000001", "2026-09-30 10:30:00"),
                                     _item("20260930103000000002", "2026-09-30 10:29:00")]}
    respx.get(JIN10_URL).mock(side_effect=[httpx.Response(200, json=PAGE1), httpx.Response(200, json=page2)])
    with caplog.at_level(logging.WARNING, logger="tidewatch.collectors.jin10"):
        result = await _collector(settings).fetch({"last_id": "20260930000000000000"})
    assert len(result.events) == 22  # 翻到上限为止的条目仍然返回
    assert any(r.levelno == logging.WARNING and "翻页" in r.getMessage() and "2" in r.getMessage() for r in caplog.records)


@respx.mock
async def test_no_page_limit_warning_when_cursor_reached(settings, caplog):
    older = {"status": 200, "data": [_item("20260930100000000000", "2026-09-30 10:00:00")]}
    respx.get(JIN10_URL).mock(side_effect=[httpx.Response(200, json=PAGE1), httpx.Response(200, json=older)])
    with caplog.at_level(logging.WARNING, logger="tidewatch.collectors.jin10"):
        await _collector(settings).fetch({"last_id": "20260930100000000000"})
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_parse_item_error_mentions_item_id():
    bad = {"id": "20260930104000000009", "type": 0, "data": {"content": "缺少时间字段"}}
    with pytest.raises(RuntimeError, match="20260930104000000009"):
        parse_item(bad)


@respx.mock
@pytest.mark.parametrize("cursor", [None, {"last_id": "20260930100000000000"}])
async def test_fetch_error_mentions_item_id_when_time_missing(settings, cursor):
    bad = {"id": "20260930104000000009", "type": 0, "data": {"content": "缺少时间字段"}}
    # 作为整页最后一条时，翻页逻辑也要读取它的时间
    page = {"status": 200, "data": [_item("20260930104100000000", "2026-09-30 10:41:00"), bad]}
    respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json=page))
    with pytest.raises(RuntimeError, match="20260930104000000009"):
        await _collector(settings).fetch(cursor)
