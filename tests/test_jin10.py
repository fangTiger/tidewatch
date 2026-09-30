import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

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
