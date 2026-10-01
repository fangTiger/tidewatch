import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from tidewatch.collectors import okx_ann
from tidewatch.collectors.okx_ann import OKX_URL, OkxAnnCollector, parse_detail

PAGE1 = json.loads((Path(__file__).parent / "fixtures" / "okx" / "announcements_page1.json").read_text(encoding="utf-8"))
DETAILS = PAGE1["data"][0]["details"]
NOW = datetime.fromtimestamp(1790661613056 / 1000 + 60, tz=timezone.utc)


def _collector(settings) -> OkxAnnCollector:
    return OkxAnnCollector(settings.collector("okx_ann"), sink=None, health=None, client=httpx.AsyncClient(trust_env=False),
                          engine=None, clock=lambda: NOW)


def test_page_limit_is_ten():
    assert okx_ann.MAX_PAGES == 10


def test_parse_details():
    events = {e.title: e for e in (parse_detail(d) for d in DETAILS)}
    lisk = events["OKX to cease deposits and withdrawals on the Lisk network"]
    assert lisk.event_type == "deposit_withdraw_suspend" and lisk.raw["exchange"] == "okx"
    assert lisk.dedupe_key == "okx_ann:https://www.okx.com/help/okx-to-cease-deposits-and-withdrawals-on-the-lisk-network"
    assert lisk.occurred_at == datetime.fromtimestamp(1790661613.056, tz=timezone.utc)
    assert events["OKX to list XDPUSD X-Perp"].event_type == "perp_listing"
    assert events["OKX to list XDPUSD X-Perp"].assets == ["XDP"]
    delist = events["OKX to delist DORA, ICX, STORJ, ZEUS and ELF spot trading pairs"]
    assert delist.event_type == "delisting" and delist.assets == ["DORA", "ICX", "STORJ", "ZEUS", "ELF"]
    assert events["USDC on Spark (Ethereum) delisting from Onchain Earn"].event_type == "other"


@respx.mock
async def test_first_run_only_within_backfill_window(settings):
    route = respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    result = await _collector(settings).fetch(None)
    assert route.call_count == 1
    assert [e.title for e in result.events] == ["OKX to cease deposits and withdrawals on the Lisk network"]
    assert result.cursor == {"last_ptime": 1790661613056}


@respx.mock
async def test_incremental(settings):
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    result = await _collector(settings).fetch({"last_ptime": 1790604011303})
    assert len(result.events) == 1 and result.cursor == {"last_ptime": 1790661613056}


@respx.mock
async def test_error_code_raises(settings):
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json={"code": "50011", "msg": "Too Many Requests"}))
    with pytest.raises(RuntimeError):
        await _collector(settings).fetch(None)


def _detail(title: str, p_time: int, slug: str) -> dict:
    return {"title": title, "annType": "announcements-new-listings", "pTime": str(p_time), "url": f"https://www.okx.com/help/{slug}"}


def _payload(details: list[dict]) -> dict:
    return {"code": "0", "msg": "", "data": [{"details": details, "totalPage": "1"}]}


@respx.mock
async def test_events_sorted_by_time_then_dedupe_key(settings):
    base = 1790661000000
    details = [  # 接口按时间倒序返回
        _detail("OKX to list AAAUSD X-Perp", base, "a"),
        _detail("OKX to list CCCUSD X-Perp", base - 1_000_000, "c"),
        _detail("OKX to list BBBUSD X-Perp", base - 1_000_000, "b"),
        _detail("OKX to list DDDUSD X-Perp", base - 5_000_000, "d"),
    ]
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=_payload(details)))
    result = await _collector(settings).fetch(None)
    assert [e.dedupe_key.rsplit("/", 1)[1] for e in result.events] == ["d", "b", "c", "a"]
    assert result.cursor == {"last_ptime": base}


@respx.mock
async def test_cursor_never_regresses_when_page_is_older_than_cursor(settings):
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    ahead = 1790700000000  # 游标比接口返回的所有公告都新（例如接口暂时返回了旧数据）
    result = await _collector(settings).fetch({"last_ptime": ahead})
    assert result.cursor == {"last_ptime": ahead}


@respx.mock
async def test_cursor_is_none_when_nothing_collected_and_no_cursor(settings):
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=_payload([])))
    result = await _collector(settings).fetch(None)
    assert result.events == [] and result.cursor is None


@respx.mock
@pytest.mark.parametrize("missing", ["pTime", "title"])
async def test_fetch_error_mentions_announcement_when_field_missing(settings, missing):
    bad = _detail("OKX to list BADUSD X-Perp", 1790661000000, "bad-announcement")
    del bad[missing]
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=_payload([_detail("OKX to list OKUSD X-Perp", 1790661000000, "ok"), bad])))
    with pytest.raises(RuntimeError, match="bad-announcement"):
        await _collector(settings).fetch(None)


# ---------------------------------------------------------------------------
# 翻页：续翻、窗口终止、触顶告警
# ---------------------------------------------------------------------------

NOW_MS = int(NOW.timestamp() * 1000)
FLOOR_MS = NOW_MS - 6 * 3600 * 1000  # 补抓窗口 6 小时


def _paged(pages: dict[int, list[dict]]):
    """按请求的 page 参数返回对应页；没有配置的页返回空列表"""

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_payload(pages.get(int(request.url.params["page"]), [])))

    return respond


def _requested_pages(route) -> list[str]:
    return [call.request.url.params["page"] for call in route.calls]


@respx.mock
async def test_paginates_while_whole_page_is_newer_than_cursor(settings, caplog):
    cursor = NOW_MS - 3_000_000
    pages = {
        1: [_detail("OKX to list P1AUSD X-Perp", NOW_MS - 100_000, "p1a"), _detail("OKX to list P1BUSD X-Perp", NOW_MS - 200_000, "p1b")],
        2: [_detail("OKX to list P2AUSD X-Perp", NOW_MS - 1_000_000, "p2a"), _detail("OKX to list OLDUSD X-Perp", cursor, "old")],  # 出现游标：追上了
        3: [_detail("OKX to list P3AUSD X-Perp", NOW_MS - 5_000_000, "p3a")],
    }
    route = respx.get(OKX_URL).mock(side_effect=_paged(pages))
    with caplog.at_level(logging.WARNING, logger="tidewatch.collectors.okx_ann"):
        result = await _collector(settings).fetch({"last_ptime": cursor})
    assert _requested_pages(route) == ["1", "2"]  # 第 2 页已追上游标，不再请求第 3 页
    # 只按补抓窗口过滤：第 2 页里与游标同时刻的条目也返回，由去重键保证只入库一次
    assert [e.dedupe_key.rsplit("/", 1)[1] for e in result.events] == ["old", "p2a", "p1b", "p1a"]
    assert result.cursor == {"last_ptime": NOW_MS - 100_000}
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@respx.mock
async def test_pagination_stops_at_backfill_window(settings, caplog):
    cursor = FLOOR_MS - 10_000_000  # 游标远早于补抓窗口
    pages = {
        1: [_detail("OKX to list NEWUSD X-Perp", NOW_MS - 100_000, "new"),
            _detail("OKX to list EDGEUSD X-Perp", FLOOR_MS - 1_000, "edge")],  # 本页最后一条已超出窗口
        2: [_detail("OKX to list NEVERUSD X-Perp", FLOOR_MS - 2_000_000, "never")],
    }
    route = respx.get(OKX_URL).mock(side_effect=_paged(pages))
    with caplog.at_level(logging.WARNING, logger="tidewatch.collectors.okx_ann"):
        result = await _collector(settings).fetch({"last_ptime": cursor})
    assert _requested_pages(route) == ["1"]  # 窗口之外不再翻页
    assert [e.dedupe_key.rsplit("/", 1)[1] for e in result.events] == ["new"]  # 窗口之外的条目不返回
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@respx.mock
async def test_pagination_stops_on_empty_page(settings):
    cursor = NOW_MS - 3_000_000
    pages = {1: [_detail("OKX to list AAAUSD X-Perp", NOW_MS - 100_000, "a")], 2: [_detail("OKX to list BBBUSD X-Perp", NOW_MS - 200_000, "b")]}
    route = respx.get(OKX_URL).mock(side_effect=_paged(pages))
    result = await _collector(settings).fetch({"last_ptime": cursor})
    assert _requested_pages(route) == ["1", "2", "3"]  # 第 3 页为空：结束
    assert len(result.events) == 2


@respx.mock
async def test_page_limit_logs_warning(settings, monkeypatch, caplog):
    monkeypatch.setattr(okx_ann, "MAX_PAGES", 2)
    cursor = NOW_MS - 3_000_000
    pages = {
        1: [_detail("OKX to list AAAUSD X-Perp", NOW_MS - 100_000, "a")],
        2: [_detail("OKX to list BBBUSD X-Perp", NOW_MS - 200_000, "b")],
        3: [_detail("OKX to list CCCUSD X-Perp", NOW_MS - 300_000, "c")],
    }
    route = respx.get(OKX_URL).mock(side_effect=_paged(pages))
    with caplog.at_level(logging.WARNING, logger="tidewatch.collectors.okx_ann"):
        result = await _collector(settings).fetch({"last_ptime": cursor})
    assert _requested_pages(route) == ["1", "2"] and len(result.events) == 2  # 翻到上限为止的条目仍然返回
    assert result.cursor == {"last_ptime": NOW_MS - 100_000}  # 触顶仍推进到本轮最新公告
    assert any(r.levelno == logging.WARNING and "翻页" in r.getMessage() and "2" in r.getMessage() for r in caplog.records)


@respx.mock
async def test_no_page_limit_warning_on_first_run(settings, monkeypatch, caplog):
    monkeypatch.setattr(okx_ann, "MAX_PAGES", 1)  # 首次运行只取第一页：即使上限只有 1 也不是「触顶」
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    with caplog.at_level(logging.WARNING, logger="tidewatch.collectors.okx_ann"):
        await _collector(settings).fetch(None)
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
