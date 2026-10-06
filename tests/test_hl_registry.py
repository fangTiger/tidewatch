from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import insert, select

from fakes import async_timeout
from tidewatch.hyperliquid.leaderboard import LeaderboardEntry
from tidewatch.hyperliquid.registry import WalletRegistry, normalize_address
from tidewatch.store.tables import hl_positions, hl_state, hl_wallets


def _address(number: int) -> str:
    return f"0x{number:040x}"


def test_normalize_address_lowercases_and_rejects_invalid_values():
    assert normalize_address("0x" + "AB" * 20) == "0x" + "ab" * 20
    for value in ("", "0X" + "ab" * 20, "0x1234", "0x" + "gg" * 20):
        with pytest.raises(ValueError, match="0x 开头的 40 位十六进制"):
            normalize_address(value)


@async_timeout()
async def test_monitor_baseline_is_invalidated_when_wallet_reenters_monitored_tier(
    engine, clock
):
    address = _address(99)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=address,
                tier="candidate",
                source="leaderboard",
            )
        )
        await conn.execute(
            insert(hl_state).values(
                name=f"monitor_baseline:{address}",
                cursor={"established_at": (clock.now - timedelta(days=1)).isoformat()},
                updated_at=clock.now - timedelta(days=1),
            )
        )

    registry = WalletRegistry(engine, clock=clock)
    assert await registry.add(address)

    async with engine.connect() as conn:
        baseline = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"monitor_baseline:{address}")
        )
    assert baseline is None

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_state).values(
                name=f"liq_risk:{address}::BTC",
                cursor={"level": "A"},
                updated_at=clock.now,
            )
        )
    assert await registry.set_excluded(address, True)
    async with engine.connect() as conn:
        risk = await conn.scalar(
            select(hl_state.c.cursor).where(
                hl_state.c.name == f"liq_risk:{address}::BTC"
            )
        )
    assert risk is None
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_state).values(
                name=f"monitor_baseline:{address}",
                cursor={"established_at": (clock.now - timedelta(days=1)).isoformat()},
                updated_at=clock.now - timedelta(days=1),
            )
        )
    assert await registry.set_excluded(address, False)
    async with engine.connect() as conn:
        restored_baseline = await conn.scalar(
            select(hl_state.c.cursor).where(hl_state.c.name == f"monitor_baseline:{address}")
        )
    assert restored_baseline is None


@async_timeout()
async def test_leaderboard_import_preserves_manual_fields_and_merges_candidates(engine, clock):
    manual, excluded, stale, discovered, newcomer, old_discovered, preserved = [
        _address(number) for number in range(1, 8)
    ]
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {
                    "address": manual,
                    "note": "我的备注",
                    "tier": "core",
                    "source": "manual",
                    "excluded": False,
                    "profile": {"win_rate": 0.8, "manual_core": True},
                },
                {
                    "address": excluded,
                    "note": None,
                    "tier": None,
                    "source": "manual",
                    "excluded": True,
                    "profile": {},
                },
                {
                    "address": stale,
                    "note": None,
                    "tier": "candidate",
                    "source": "leaderboard",
                    "excluded": False,
                    "profile": {},
                },
                {
                    "address": discovered,
                    "note": None,
                    "tier": "candidate",
                    "source": "discovered",
                    "excluded": False,
                    "profile": {"last_trade_at": (clock.now - timedelta(days=20)).isoformat()},
                },
                {
                    "address": old_discovered,
                    "note": None,
                    "tier": "candidate",
                    "source": "discovered",
                    "excluded": False,
                    "profile": {"last_trade_at": (clock.now - timedelta(days=31)).isoformat()},
                },
                {
                    "address": preserved,
                    "note": "保留我的备注",
                    "tier": "candidate",
                    "source": "leaderboard",
                    "excluded": False,
                    "profile": {
                        "win_rate": 0.7,
                        "leaderboard_account_value": "3000000",
                        "leaderboard_display_name": "旧榜单名",
                        "hypurrscan_alias": "旧别名",
                    },
                },
            ],
        )

    registry = WalletRegistry(engine, clock=clock)
    result = await registry.import_leaderboard(
        [
            LeaderboardEntry(manual, Decimal("9000000"), "排行榜名"),
            LeaderboardEntry(excluded, Decimal("8000000"), "不应导入"),
            LeaderboardEntry(newcomer, Decimal("7000000"), None),
        ],
        aliases={manual: "外部别名", newcomer: "新地址别名"},
    )

    assert result.imported == 2 and result.removed == 2
    async with engine.connect() as conn:
        rows = {
            row.address: row
            for row in (await conn.execute(select(hl_wallets).order_by(hl_wallets.c.address))).all()
        }
    assert set(rows) == {manual, excluded, discovered, newcomer, preserved}
    assert rows[manual].note == "我的备注"
    assert rows[manual].tier == "core" and rows[manual].source == "manual"
    assert rows[manual].profile == {
        "win_rate": 0.8,
        "manual_core": True,
        "leaderboard_display_name": "排行榜名",
        "hypurrscan_alias": "外部别名",
        "leaderboard_account_value": "9000000",
    }
    assert rows[excluded].excluded is True and rows[excluded].tier is None
    assert rows[discovered].source == "discovered"
    assert rows[newcomer].tier == "candidate" and rows[newcomer].source == "leaderboard"
    assert rows[preserved].note == "保留我的备注" and rows[preserved].profile == {"win_rate": 0.7}


@async_timeout()
async def test_discovered_address_on_current_leaderboard_is_not_removed_when_trade_is_old(engine, clock):
    address = _address(8)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=address,
                tier="candidate",
                source="discovered",
                profile={"last_trade_at": (clock.now - timedelta(days=31)).isoformat()},
            )
        )
    registry = WalletRegistry(engine, clock=clock)
    await registry.import_leaderboard([LeaderboardEntry(address, Decimal("2000000"), "重新上榜")])
    rows = await registry.list_wallets()
    assert len(rows) == 1 and rows[0].address == address
    assert rows[0].profile["leaderboard_display_name"] == "重新上榜"


@async_timeout()
async def test_successful_label_refresh_clears_withdrawn_external_labels(engine, clock):
    address = _address(9)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=address,
                tier="candidate",
                source="leaderboard",
                note="用户备注",
                profile={
                    "leaderboard_display_name": "旧榜单名",
                    "hypurrscan_alias": "旧别名",
                    "win_rate": 0.6,
                },
            )
        )

    registry = WalletRegistry(engine, clock=clock)
    await registry.import_leaderboard(
        [LeaderboardEntry(address, Decimal("2000000"), None)],
        aliases={},
        aliases_authoritative=True,
    )

    row = (await registry.list_wallets())[0]
    assert row.note == "用户备注"
    assert row.profile == {"leaderboard_account_value": "2000000", "win_rate": 0.6}


@async_timeout()
async def test_stale_leaderboard_wallet_with_style_profile_is_preserved(engine, clock):
    address = _address(15)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=address,
                tier="candidate",
                source="leaderboard",
                style="directional",
                profile={},
                profile_updated_at=clock.now,
            )
        )

    registry = WalletRegistry(engine, clock=clock)
    result = await registry.import_leaderboard([])
    rows = await registry.list_wallets()
    assert result.removed == 0
    assert len(rows) == 1 and rows[0].style == "directional"


@async_timeout()
async def test_manual_crud_is_idempotent_and_enforces_conflicts(engine, clock):
    registry = WalletRegistry(engine, clock=clock, core_limit=2)
    first, second, third = _address(10), _address(11), _address(12)

    assert await registry.add(first) is True
    assert await registry.add(first) is False
    await registry.set_label(first, "主账户")
    await registry.set_core(first)
    await registry.set_core(first)
    await registry.set_core(second)
    with pytest.raises(ValueError, match="核心钱包最多 2 个"):
        await registry.set_core(third)
    with pytest.raises(ValueError, match="核心钱包不能直接排除"):
        await registry.set_excluded(first, True)

    await registry.set_core(first, False)
    await registry.set_excluded(first, True)
    await registry.set_excluded(first, True)
    with pytest.raises(ValueError, match="已排除"):
        await registry.add(first)
    await registry.set_excluded(first, False)
    assert await registry.remove(first) is True
    assert await registry.remove(first) is False

    rows = await registry.list_wallets()
    assert [(row.address, row.tier) for row in rows] == [(second, "core")]


@async_timeout()
async def test_manual_add_replaces_lowest_automatic_focus_but_not_manual(engine, clock):
    manual, automatic, newcomer = _address(20), _address(21), _address(22)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {"address": manual, "tier": "focus", "source": "manual"},
                {"address": automatic, "tier": "focus", "source": "leaderboard"},
            ],
        )
        await conn.execute(
            insert(hl_positions),
            [
                {
                    "address": manual,
                    "dex": "",
                    "coin": "BTC",
                    "szi": 1,
                    "notional_usd": 1,
                    "updated_at": clock.now,
                },
                {
                    "address": automatic,
                    "dex": "",
                    "coin": "ETH",
                    "szi": 1,
                    "notional_usd": 2,
                    "updated_at": clock.now,
                },
            ],
        )

    registry = WalletRegistry(engine, clock=clock, focus_limit=2)
    await registry.add(newcomer)
    rows = {row.address: row for row in await registry.list_wallets()}
    assert rows[manual].tier == "focus"
    assert rows[newcomer].tier == "focus"
    assert rows[automatic].tier == "candidate"


@async_timeout()
async def test_add_then_remove_restores_automatic_relationship_and_profile(engine, clock):
    address = _address(23)
    original_profile = {"win_rate": 0.75, "leaderboard_display_name": "榜单名"}
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=address,
                tier="candidate",
                source="leaderboard",
                profile=original_profile,
            )
        )
    registry = WalletRegistry(engine, clock=clock)
    assert await registry.add(address) is True
    assert await registry.remove(address) is True
    rows = await registry.list_wallets()
    assert len(rows) == 1
    assert rows[0].source == "leaderboard" and rows[0].tier == "candidate"
    assert rows[0].profile == original_profile


@async_timeout()
async def test_configured_core_sync_is_atomic_and_remove_cannot_override_config(engine, clock):
    configured = {_address(30), _address(31)}
    registry = WalletRegistry(engine, configured_core=configured, clock=clock, core_limit=2)
    await registry.sync_configured_core()
    await registry.sync_configured_core()
    assert await registry.remove(_address(30)) is False
    assert {row.address for row in await registry.list_wallets(tier="core")} == configured

    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets).values(
                address=_address(32),
                tier="core",
                source="manual",
                profile={"manual_core": True},
            )
        )
    with pytest.raises(ValueError, match="配置与数据库中的核心钱包合计超过 2 个"):
        await registry.sync_configured_core()

    assert {row.address for row in await registry.list_wallets(tier="core")} == {
        *configured,
        _address(32),
    }


@async_timeout()
async def test_removing_configured_core_restores_manual_focus(engine, clock):
    address = _address(33)
    manual = WalletRegistry(engine, clock=clock)
    await manual.add(address)
    configured = WalletRegistry(engine, configured_core=[address], clock=clock)
    await configured.sync_configured_core()
    await manual.sync_configured_core()

    row = (await manual.list_wallets())[0]
    assert row.tier == "focus"
    assert row.profile == {"manual_focus": True}


@async_timeout()
async def test_concurrent_core_additions_cannot_exceed_limit(engine, clock):
    await _insert_core_rows(engine, range(50, 59))
    first = WalletRegistry(engine, clock=clock, core_limit=10)
    second = WalletRegistry(engine, clock=clock, core_limit=10)

    results = await asyncio.gather(
        first.set_core(_address(59)),
        second.set_core(_address(60)),
        return_exceptions=True,
    )
    assert sum(result is True for result in results) == 1
    assert sum(isinstance(result, ValueError) for result in results) == 1
    assert len(await first.list_wallets(tier="core")) == 10


async def _insert_core_rows(engine, numbers) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {
                    "address": _address(number),
                    "tier": "core",
                    "source": "manual",
                    "profile": {"manual_core": True},
                }
                for number in numbers
            ],
        )


@async_timeout()
async def test_list_filters_and_name_priority(engine, clock):
    first, second = _address(40), _address(41)
    async with engine.begin() as conn:
        await conn.execute(
            insert(hl_wallets),
            [
                {
                    "address": first,
                    "note": "用户备注",
                    "tier": "focus",
                    "source": "manual",
                    "excluded": False,
                    "style": "directional",
                    "profile": {
                        "leaderboard_display_name": "榜单名",
                        "hypurrscan_alias": "别名",
                    },
                },
                {
                    "address": second,
                    "note": None,
                    "tier": None,
                    "source": "leaderboard",
                    "excluded": True,
                    "style": None,
                    "profile": {"leaderboard_display_name": "仅榜单名"},
                },
            ],
        )
        await conn.execute(
            insert(hl_positions).values(
                address=first,
                dex="",
                coin="BTC",
                szi=1,
                notional_usd=Decimal("123.45"),
                updated_at=clock.now,
            )
        )

    registry = WalletRegistry(engine, clock=clock)
    rows = await registry.list_wallets(tier="focus", excluded=False)
    assert len(rows) == 1
    assert rows[0].display_name == "用户备注"
    assert rows[0].external_labels == ("榜单名", "别名")
    assert rows[0].total_notional == Decimal("123.45")
    assert [row.display_name for row in await registry.list_wallets(excluded=True)] == ["仅榜单名"]
