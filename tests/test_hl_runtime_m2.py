from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import insert
from sqlalchemy import select

from fakes import async_timeout
from tidewatch.hyperliquid.runtime import HyperliquidRuntime, seconds_until_shanghai_8
from tidewatch.store.tables import hl_state, hl_wallets


class _Health:
    def register(self, name: str, stale_after: float) -> None:
        pass

    async def success(self, name: str) -> None:
        pass

    async def failure(self, name: str, error: str) -> None:
        pass


def test_daily_recompute_targets_0800_asia_shanghai():
    before = datetime(2026, 10, 6, 22, 30, tzinfo=timezone.utc)  # 上海次日 06:30
    exact = datetime(2026, 10, 7, 0, 0, tzinfo=timezone.utc)  # 上海 08:00
    assert seconds_until_shanghai_8(before) == 90 * 60
    assert seconds_until_shanghai_8(exact) == 0


@async_timeout()
async def test_runtime_logs_proxy_ip_notice_once_on_start(engine, settings, clock, caplog):
    with caplog.at_level(logging.INFO):
        runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)

        async def idle():
            await runtime._stop.wait()

        runtime.run_rest = idle
        runtime.run_trades_ws = idle
        runtime.run_core_ws = idle
        runtime.run_leaderboard = idle
        runtime.run_tier_recompute = idle
        runtime.run_profile = idle
        runtime.run_market_data = idle
        await asyncio.wait_for(runtime.start(), timeout=1)
        await asyncio.wait_for(runtime.stop(), timeout=1)

    notices = [
        record
        for record in caplog.records
        if "切换代理节点会改变限流出口 IP" in record.getMessage()
    ]
    assert len(notices) == 1
    assert notices[0].name == "tidewatch.hyperliquid.runtime"


@async_timeout()
async def test_runtime_cleans_liquidation_risk_orphans_before_starting_tasks(
    engine, settings, clock
):
    runtime = HyperliquidRuntime(
        settings.hyperliquid,
        _Health(),
        engine=engine,
        clock=clock,
    )
    cleanup_calls = 0

    async def cleanup_liquidation_risk_states() -> int:
        nonlocal cleanup_calls
        cleanup_calls += 1
        return 0

    async def idle() -> None:
        await runtime._stop.wait()

    runtime.monitor.cleanup_liquidation_risk_states = cleanup_liquidation_risk_states
    for name in (
        "run_rest",
        "run_trades_ws",
        "run_core_ws",
        "run_trade_messages",
        "run_discovery_cleanup",
        "run_core_subscriptions",
        "run_core_messages",
        "run_leaderboard",
        "run_tier_recompute",
        "run_profile",
        "run_market_data",
    ):
        setattr(runtime, name, idle)

    await asyncio.wait_for(runtime.start(), timeout=1)
    await asyncio.wait_for(runtime.stop(), timeout=1)

    assert cleanup_calls == 1


@async_timeout()
async def test_empty_registry_imports_immediately_then_recomputes(engine, settings, clock):
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    imported = asyncio.Event()
    recomputed = asyncio.Event()
    order: list[str] = []

    async def import_once():
        order.append("import")
        imported.set()

    async def recompute():
        order.append("recompute")
        recomputed.set()

    runtime.importer.import_once = import_once
    runtime.tiers.recompute = recompute
    task = asyncio.create_task(runtime.run_leaderboard())
    try:
        await asyncio.wait_for(imported.wait(), timeout=1)
        await asyncio.wait_for(recomputed.wait(), timeout=1)
        assert order == ["import", "recompute"]
    finally:
        runtime._stop.set()
        await asyncio.wait_for(task, timeout=1)
        await runtime.close_resources()


@async_timeout()
async def test_existing_candidate_waits_for_twelve_hour_schedule(engine, settings, clock):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address="0x0000000000000000000000000000000000000001",
                tier="candidate",
                source="leaderboard",
            )
        )
        await conn.execute(
            insert(hl_state).values(
                name="leaderboard_import",
                cursor={"phase": "complete", "completed_at": clock.now.isoformat()},
                updated_at=clock.now,
            )
        )
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    called = asyncio.Event()

    async def import_once():
        called.set()

    runtime.importer.import_once = import_once
    task = asyncio.create_task(runtime.run_leaderboard())
    try:
        try:
            await asyncio.wait_for(called.wait(), timeout=0.05)
            assert False, "已有候选时不应在启动瞬间导入"
        except asyncio.TimeoutError:
            pass
    finally:
        runtime._stop.set()
        await asyncio.wait_for(task, timeout=1)
        await runtime.close_resources()


@async_timeout()
async def test_overdue_leaderboard_cursor_imports_immediately(engine, settings, clock):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address="0x0000000000000000000000000000000000000002",
                tier="candidate",
                source="leaderboard",
            )
        )
        await conn.execute(
            insert(hl_state).values(
                name="leaderboard_import",
                cursor={
                    "phase": "complete",
                    "completed_at": (clock.now - timedelta(hours=13)).isoformat(),
                },
                updated_at=clock.now - timedelta(hours=13),
            )
        )
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    called = asyncio.Event()

    async def import_once():
        called.set()

    async def recompute():
        return None

    runtime.importer.import_once = import_once
    runtime.tiers.recompute = recompute
    task = asyncio.create_task(runtime.run_leaderboard())
    try:
        await asyncio.wait_for(called.wait(), timeout=1)
    finally:
        runtime._stop.set()
        await asyncio.wait_for(task, timeout=1)
        await runtime.close_resources()


@async_timeout()
async def test_tier_failure_after_import_retries_without_redownloading(engine, settings, clock):
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    delays: list[float] = []
    imports = 0
    recomputes = 0

    async def import_once():
        nonlocal imports
        imports += 1

    async def recompute():
        nonlocal recomputes
        recomputes += 1
        if recomputes == 1:
            raise RuntimeError("分层失败")

    async def wait_or_stop(delay: float) -> bool:
        delays.append(delay)
        if len(delays) == 2:
            runtime._stop.set()
            return True
        return False

    runtime.importer.import_once = import_once
    runtime.tiers.recompute = recompute
    runtime._wait_or_stop = wait_or_stop
    try:
        await asyncio.wait_for(runtime.run_leaderboard(), timeout=1)
    finally:
        await runtime.close_resources()

    assert imports == 1
    assert recomputes == 2
    assert delays == [300, settings.hyperliquid.frequencies.leaderboard_interval_s]


@async_timeout()
async def test_incomplete_leaderboard_cursor_resumes_immediately(engine, settings, clock):
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address="0x0000000000000000000000000000000000000003",
                tier="candidate",
                source="leaderboard",
            )
        )
        await conn.execute(
            insert(hl_state).values(
                name="leaderboard_import",
                cursor={"phase": "merging", "candidate_count": 10},
                updated_at=clock.now,
            )
        )
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    called = asyncio.Event()

    async def import_once():
        called.set()

    async def recompute():
        return None

    runtime.importer.import_once = import_once
    runtime.tiers.recompute = recompute
    task = asyncio.create_task(runtime.run_leaderboard())
    try:
        await asyncio.wait_for(called.wait(), timeout=1)
    finally:
        runtime._stop.set()
        await asyncio.wait_for(task, timeout=1)
        await runtime.close_resources()


@async_timeout()
async def test_daily_tier_failure_uses_short_retry(engine, settings, clock):
    clock.now = datetime(2026, 10, 7, 0, 0, tzinfo=timezone.utc)
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    delays: list[float] = []
    recomputes = 0

    async def recompute():
        nonlocal recomputes
        recomputes += 1
        if recomputes == 1:
            raise RuntimeError("分层失败")

    async def wait_or_stop(delay: float) -> bool:
        delays.append(delay)
        if len(delays) == 3:
            runtime._stop.set()
            return True
        return False

    runtime.tiers.recompute = recompute
    runtime._wait_or_stop = wait_or_stop
    try:
        await asyncio.wait_for(runtime.run_tier_recompute(), timeout=1)
    finally:
        await runtime.close_resources()

    assert recomputes == 2
    assert delays == [0, 300, settings.hyperliquid.frequencies.tier_recompute_interval_s]


@async_timeout()
async def test_runtime_wires_candidate_threshold_and_limit(engine, settings, clock):
    settings.hyperliquid.tiers.candidate_limit = 17
    settings.hyperliquid.events.discovery_account_value_usd = 1_234_567
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    try:
        assert runtime.importer.candidate_limit == 17
        assert runtime.importer.minimum_value == 1_234_567
    finally:
        await runtime.close_resources()


@async_timeout()
async def test_start_syncs_configured_core_before_spawning_tasks(engine, settings, clock):
    configured = "0x0000000000000000000000000000000000000064"
    settings.hyperliquid.core_wallets = [configured]
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address="0x0000000000000000000000000000000000000065",
                tier="candidate",
                source="leaderboard",
            )
        )
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    await asyncio.wait_for(runtime.start(), timeout=1)
    try:
        assert "hl-discovery-cleanup" in {
            task.get_name() for task in runtime.background_tasks()
        }
        async with engine.connect() as conn:
            row = (
                await conn.execute(select(hl_wallets).where(hl_wallets.c.address == configured))
            ).one()
        assert row.tier == "core" and row.profile["configured_core"] is True
    finally:
        await asyncio.wait_for(runtime.stop(), timeout=1)


@async_timeout()
async def test_start_rejects_combined_core_limit_atomically(engine, settings, clock):
    rows = [
        {
            "address": f"0x{number:040x}",
            "tier": "core",
            "source": "manual",
            "profile": {"manual_core": True},
        }
        for number in range(1, 11)
    ]
    async with engine.begin() as conn:
        await conn.execute(insert(hl_wallets), rows)
    settings.hyperliquid.core_wallets = ["0x0000000000000000000000000000000000000064"]
    runtime = HyperliquidRuntime(settings.hyperliquid, _Health(), engine=engine, clock=clock)
    try:
        with pytest.raises(ValueError, match="配置与数据库中的核心钱包合计超过 10 个"):
            await asyncio.wait_for(runtime.start(), timeout=1)
        assert runtime.background_tasks() == ()
    finally:
        await runtime.close_resources()


def test_data_directory_is_ignored_but_fixtures_are_trackable():
    root = Path(__file__).resolve().parents[1]
    patterns = (root / ".gitignore").read_text().splitlines()
    assert "data/" in patterns
    assert not any(pattern.startswith("tests/fixtures/hyperliquid") for pattern in patterns)
