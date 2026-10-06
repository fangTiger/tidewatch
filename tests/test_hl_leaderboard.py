from __future__ import annotations

import asyncio
import json
import tracemalloc
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from fakes import async_timeout
from tidewatch.hyperliquid.leaderboard import (
    HLP_ADDRESSES,
    LeaderboardEntry,
    LeaderboardSource,
    iter_leaderboard_rows,
    select_top_candidates,
)


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def test_real_fixtures_are_small_and_cover_recorded_shapes():
    """约束类测试：夹具来自真实响应，但不能把 40MB 原文提交进仓库。"""
    files = list(FIXTURES.iterdir())
    assert files and all(path.stat().st_size < 300_000 for path in files)

    rows = json.loads((FIXTURES / "leaderboard_sample.json").read_text())["leaderboardRows"]
    values = [Decimal(row["accountValue"]) for row in rows]
    assert min(values) < 1_000_000 <= max(values)
    assert {row["displayName"] is None for row in rows} == {True, False}

    vault = json.loads((FIXTURES / "hlp_vault_details.json").read_text())
    children = vault["relationship"]["data"]["childAddresses"]
    assert vault["vaultAddress"] in HLP_ADDRESSES
    assert set(children) < HLP_ADDRESSES


@async_timeout()
async def test_parser_yields_before_complete_body_is_consumed():
    """解析器必须在后续分块尚未到达时先产出首行，不能聚合完整正文。"""
    body = (FIXTURES / "leaderboard_sample.json").read_bytes()
    chunks = [body[index : index + 37] for index in range(0, len(body), 37)]
    consumed = 0

    async def stream():
        nonlocal consumed
        for chunk in chunks:
            consumed += 1
            yield chunk

    rows = iter_leaderboard_rows(stream())
    first = await anext(rows)
    assert first["ethAddress"] == "0x7b7f72a28fe109fa703eeed7984f2a8a68fedee2"
    assert consumed < len(chunks)
    assert len([first, *[row async for row in rows]]) == 4


def test_selection_applies_threshold_protocol_exclusion_and_deterministic_limit():
    protocol = sorted(HLP_ADDRESSES)
    rows = [
        {"ethAddress": protocol[0], "accountValue": "9000000", "displayName": "HLP"},
        {"ethAddress": "0x0000000000000000000000000000000000000002", "accountValue": "1000000", "displayName": None},
        {"ethAddress": "0x0000000000000000000000000000000000000001", "accountValue": "1000000", "displayName": "甲"},
        {"ethAddress": "0x0000000000000000000000000000000000000003", "accountValue": "999999.99", "displayName": None},
        {"ethAddress": "0x0000000000000000000000000000000000000004", "accountValue": "8000000", "displayName": None},
    ]
    result = select_top_candidates(
        rows,
        excluded_addresses={"0x0000000000000000000000000000000000000004"},
        limit=2,
    )
    assert result == [
        LeaderboardEntry("0x0000000000000000000000000000000000000001", Decimal("1000000"), "甲"),
        LeaderboardEntry("0x0000000000000000000000000000000000000002", Decimal("1000000"), None),
    ]


def test_selection_counts_unique_addresses_toward_limit():
    first = "0x0000000000000000000000000000000000000011"
    second = "0x0000000000000000000000000000000000000012"
    rows = [
        {"ethAddress": first, "accountValue": "3000000", "displayName": "旧名"},
        {"ethAddress": first, "accountValue": "4000000", "displayName": "新名"},
        {"ethAddress": second, "accountValue": "2000000", "displayName": None},
    ]
    result = select_top_candidates(rows, limit=2)
    assert result == [
        LeaderboardEntry(first, Decimal("4000000"), "新名"),
        LeaderboardEntry(second, Decimal("2000000"), None),
    ]


def test_selection_keeps_exact_top_thousand_from_larger_pool():
    rows = [
        {
            "ethAddress": f"0x{index:040x}",
            "accountValue": str(1_000_000 + index),
            "displayName": None,
        }
        for index in range(1, 1_201)
    ]
    result = select_top_candidates(rows, limit=1_000)
    assert len(result) == 1_000
    assert result[0].account_value == Decimal("1001200")
    assert result[-1].account_value == Decimal("1000201")


class _ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.yielded = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk


@async_timeout()
async def test_source_uses_http_stream_without_response_json():
    body = (FIXTURES / "leaderboard_sample.json").read_bytes()
    stream = _ChunkStream([body[index : index + 61] for index in range(0, len(body), 61)])

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        source = LeaderboardSource(client=client, url="https://example.test/leaderboard")
        result = await asyncio.wait_for(source.fetch_top(limit=10), timeout=1)

    assert [entry.address for entry in result] == [
        "0x85ecf584f25db6f146718b86d493e33c5af72052",
        "0x7b7f72a28fe109fa703eeed7984f2a8a68fedee2",
    ]
    assert stream.yielded > 1


@async_timeout()
async def test_parser_rejects_unbounded_single_row():
    async def stream():
        yield b'{"leaderboardRows":[{"ethAddress":"0x'
        yield b"a" * 2048

    with pytest.raises(ValueError, match="单条记录超过"):
        async for _ in iter_leaderboard_rows(stream(), max_buffer_chars=1024):
            pass


@async_timeout(60)
async def test_synthetic_forty_mb_leaderboard_peak_memory_below_limit():
    """流式合成大响应，不在仓库落盘；验证解析峰值内存与 Top-N。"""
    row_count = 55_000
    padding = "榜" * 220
    emitted = 0

    class GeneratedStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            nonlocal emitted
            yield b'{"leaderboardRows":['
            for index in range(row_count):
                row = {
                    "ethAddress": f"0x{index + 1:040x}",
                    "accountValue": str(1_000_000 + index),
                    "displayName": padding,
                }
                chunk = (("," if index else "") + json.dumps(row, ensure_ascii=False)).encode()
                emitted += len(chunk)
                yield chunk
            yield b"]}"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=GeneratedStream())

    tracemalloc.start()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await LeaderboardSource(
                client=client,
                url="https://example.test/leaderboard",
            ).fetch_top(limit=10)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert emitted >= 40 * 1024 * 1024
    assert peak < 32 * 1024 * 1024
    assert [entry.account_value for entry in result] == [
        Decimal(1_000_000 + index)
        for index in range(row_count - 1, row_count - 11, -1)
    ]
