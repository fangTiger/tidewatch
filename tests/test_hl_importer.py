from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from fakes import async_timeout
from tidewatch.hyperliquid.importer import AliasCache, AliasResult, LeaderboardImporter
from tidewatch.hyperliquid.leaderboard import LeaderboardEntry
from tidewatch.hyperliquid.registry import WalletRegistry
from tidewatch.store.tables import hl_state, hl_wallets


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


class _Health:
    def __init__(self) -> None:
        self.successes: list[str] = []
        self.failures: list[tuple[str, str]] = []

    async def success(self, source: str) -> None:
        self.successes.append(source)

    async def failure(self, source: str, error: str) -> None:
        self.failures.append((source, error))


@async_timeout()
async def test_alias_cache_refreshes_daily_and_reuses_fresh_file(tmp_path, clock):
    payload = (FIXTURES / "global_aliases_sample.json").read_bytes()
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, content=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        cache = AliasCache(tmp_path / "labels", client=client, clock=clock)
        first = await asyncio.wait_for(cache.load(), timeout=1)
        second = await asyncio.wait_for(cache.load(), timeout=1)

    assert requests == 1
    assert first == second
    assert first.refresh_failed is False
    assert first.aliases["0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"] == "HLP Vault"
    stored = json.loads((tmp_path / "labels" / "hypurrscan-global-aliases.json").read_text())
    assert stored["fetched_at"] == clock.now.isoformat()


@async_timeout()
async def test_alias_cache_failure_uses_old_cache(tmp_path, clock):
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir()
    cached = {
        "fetched_at": (clock.now - timedelta(days=2)).isoformat(),
        "aliases": {"0x0000000000000000000000000000000000000001": "旧标签"},
    }
    (labels_dir / "hypurrscan-global-aliases.json").write_text(json.dumps(cached))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="unavailable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await asyncio.wait_for(
            AliasCache(labels_dir, client=client, clock=clock).load(),
            timeout=1,
        )

    assert result.aliases == cached["aliases"]
    assert result.refresh_failed is True and "503" in (result.error or "")


@async_timeout()
async def test_importer_updates_cursor_and_success_even_when_no_rows(engine, clock):
    class EmptySource:
        async def fetch_top(self, **kwargs):
            return []

    class EmptyAliases:
        async def load(self):
            return AliasResult({}, False, None)

    health = _Health()
    importer = LeaderboardImporter(
        WalletRegistry(engine, clock=clock),
        EmptySource(),
        EmptyAliases(),
        health=health,
        clock=clock,
    )
    result = await importer.import_once()

    assert result.imported == 0
    assert health.successes == ["hl_leaderboard"] and health.failures == []
    async with engine.connect() as conn:
        state = (await conn.execute(select(hl_state).where(hl_state.c.name == "leaderboard_import"))).one()
    assert state.cursor == {
        "phase": "complete",
        "completed_at": clock.now.isoformat(),
        "imported": 0,
        "removed": 0,
    }


@async_timeout()
async def test_importer_persists_downloading_progress_before_http_finishes(engine, clock):
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowSource:
        async def fetch_top(self, **kwargs):
            entered.set()
            await release.wait()
            return []

    class EmptyAliases:
        async def load(self):
            return AliasResult({}, False, None)

    importer = LeaderboardImporter(
        WalletRegistry(engine, clock=clock),
        SlowSource(),
        EmptyAliases(),
        clock=clock,
    )
    task = asyncio.create_task(importer.import_once())
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        async with engine.connect() as conn:
            cursor = (
                await conn.execute(select(hl_state.c.cursor).where(hl_state.c.name == "leaderboard_import"))
            ).scalar_one()
        assert cursor == {"phase": "downloading", "started_at": clock.now.isoformat()}
        release.set()
        await asyncio.wait_for(task, timeout=1)
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@async_timeout()
async def test_alias_failure_does_not_block_import_but_reports_health(engine, clock):
    address = "0x0000000000000000000000000000000000000001"

    class Source:
        async def fetch_top(self, **kwargs):
            return [LeaderboardEntry(address, Decimal("2000000"), "榜单名")]

    class FailedAliases:
        async def load(self):
            return AliasResult({address: "旧别名"}, True, "别名接口超时")

    health = _Health()
    importer = LeaderboardImporter(
        WalletRegistry(engine, clock=clock),
        Source(),
        FailedAliases(),
        health=health,
        clock=clock,
    )
    await importer.import_once()

    assert health.successes == []
    assert health.failures == [("hl_leaderboard", "别名接口超时")]
    async with engine.connect() as conn:
        row = (await conn.execute(select(hl_wallets))).one()
    assert row.profile["leaderboard_display_name"] == "榜单名"
    assert row.profile["hypurrscan_alias"] == "旧别名"


@async_timeout()
async def test_leaderboard_failure_reports_health_and_keeps_existing_rows(engine, clock):
    address = "0x0000000000000000000000000000000000000002"
    registry = WalletRegistry(engine, clock=clock)
    await registry.add(address)

    class BrokenSource:
        async def fetch_top(self, **kwargs):
            raise httpx.ConnectError("连接失败")

    class Aliases:
        async def load(self):
            raise AssertionError("排行榜失败后不应刷新别名")

    health = _Health()
    importer = LeaderboardImporter(registry, BrokenSource(), Aliases(), health=health, clock=clock)
    with pytest.raises(httpx.ConnectError):
        await importer.import_once()

    assert health.successes == []
    assert health.failures and "连接失败" in health.failures[0][1]
    assert [row.address for row in await registry.list_wallets()] == [address]


@async_timeout()
async def test_unexpected_alias_failure_reports_health_and_keeps_existing_rows(engine, clock):
    address = "0x0000000000000000000000000000000000000003"
    registry = WalletRegistry(engine, clock=clock)
    await registry.add(address)

    class Source:
        async def fetch_top(self, **kwargs):
            return []

    class BrokenAliases:
        async def load(self):
            raise RuntimeError("别名缓存损坏")

    health = _Health()
    importer = LeaderboardImporter(registry, Source(), BrokenAliases(), health=health, clock=clock)
    with pytest.raises(RuntimeError, match="别名缓存损坏"):
        await importer.import_once()

    assert health.successes == []
    assert health.failures and "别名缓存损坏" in health.failures[0][1]
    assert [row.address for row in await registry.list_wallets()] == [address]


@async_timeout()
async def test_importer_passes_configured_threshold_and_limit_to_source(engine, clock):
    calls = []

    class CapturingSource:
        async def fetch_top(self, **kwargs):
            calls.append(kwargs)
            return []

    class EmptyAliases:
        async def load(self):
            return AliasResult({}, False, None)

    importer = LeaderboardImporter(
        WalletRegistry(engine, clock=clock),
        CapturingSource(),
        EmptyAliases(),
        candidate_limit=17,
        minimum_value=Decimal("1234567"),
        clock=clock,
    )
    await importer.import_once()

    assert calls == [
        {
            "excluded_addresses": set(),
            "limit": 17,
            "minimum_value": Decimal("1234567"),
        }
    ]
