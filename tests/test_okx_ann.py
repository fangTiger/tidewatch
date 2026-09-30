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
