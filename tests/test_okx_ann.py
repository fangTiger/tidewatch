import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from tidewatch.collectors.okx_ann import OKX_URL, OkxAnnCollector, parse_detail

PAGE1 = json.loads((Path(__file__).parent / "fixtures" / "okx" / "announcements_page1.json").read_text(encoding="utf-8"))
DETAILS = PAGE1["data"][0]["details"]
NOW = datetime.fromtimestamp(1790661613056 / 1000 + 60, tz=timezone.utc)


def _collector(settings) -> OkxAnnCollector:
    return OkxAnnCollector(settings.collector("okx_ann"), sink=None, health=None, client=httpx.AsyncClient(trust_env=False),
                           engine=None, clock=lambda: NOW)


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
