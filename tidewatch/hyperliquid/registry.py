"""Hyperliquid 巨鲸名单的共享服务层。"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.hyperliquid.leaderboard import LeaderboardEntry
from tidewatch.store.tables import hl_positions, hl_wallets


_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")
REGISTRY_LOCK_KEY = 0x74696465
_AUTOMATIC_PROFILE_KEYS = {
    "last_trade_at",
    "leaderboard_account_value",
    "leaderboard_display_name",
    "hypurrscan_alias",
}


def normalize_address(value: str) -> str:
    """校验并返回小写的 20 字节十六进制地址。"""
    if not isinstance(value, str) or _ADDRESS_RE.fullmatch(value) is None:
        raise ValueError("钱包地址必须是 0x 开头的 40 位十六进制地址")
    return value.lower()


@dataclass(frozen=True)
class ImportResult:
    """一次排行榜名单合并的统计。"""

    imported: int
    removed: int


@dataclass(frozen=True)
class WalletView:
    """供 CLI 与网站共同使用的钱包展示对象。"""

    address: str
    note: str | None
    tier: str | None
    source: str
    excluded: bool
    style: str | None
    profile: Mapping[str, Any]
    total_notional: Decimal

    @property
    def external_labels(self) -> tuple[str, ...]:
        labels: list[str] = []
        for key in ("leaderboard_display_name", "hypurrscan_alias"):
            value = self.profile.get(key)
            if isinstance(value, str) and value and value not in labels:
                labels.append(value)
        return tuple(labels)

    @property
    def display_name(self) -> str | None:
        return self.note or next(iter(self.external_labels), None)


class WalletRegistry:
    """封装名单写入、校验和查询，CLI 与网站不得绕过此层。"""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        configured_core: Iterable[str] = (),
        core_limit: int = 10,
        focus_limit: int = 200,
        clock: Clock = utcnow,
    ) -> None:
        self.engine = engine
        self.configured_core = frozenset(normalize_address(address) for address in configured_core)
        self.core_limit = core_limit
        self.focus_limit = focus_limit
        self.clock = clock

    async def import_leaderboard(
        self,
        entries: Iterable[LeaderboardEntry],
        *,
        aliases: Mapping[str, str] | None = None,
        aliases_authoritative: bool = True,
    ) -> ImportResult:
        """合并排行榜候选，同时保留人工字段、发现来源与画像。"""
        normalized_aliases = {
            normalize_address(address): label
            for address, label in (aliases or {}).items()
            if isinstance(address, str) and isinstance(label, str) and label
        }
        entry_list = list(entries)
        imported_addresses: set[str] = set()
        imported = 0
        removed = 0
        async with self.engine.begin() as conn:
            await self._lock_registry(conn)
            current_rows = (
                await conn.execute(select(hl_wallets).with_for_update())
            ).mappings().all()
            current = {row["address"]: row for row in current_rows}
            for entry in entry_list:
                address = normalize_address(entry.address)
                existing = current.get(address)
                if existing is not None and existing["excluded"]:
                    continue
                profile = dict(existing["profile"] or {}) if existing is not None else {}
                profile["leaderboard_account_value"] = str(entry.account_value)
                if entry.display_name:
                    profile["leaderboard_display_name"] = entry.display_name
                else:
                    profile.pop("leaderboard_display_name", None)
                alias = normalized_aliases.get(address)
                if alias:
                    profile["hypurrscan_alias"] = alias
                elif aliases_authoritative:
                    profile.pop("hypurrscan_alias", None)
                if existing is None:
                    await conn.execute(
                        insert(hl_wallets).values(
                            address=address,
                            tier="candidate",
                            source="leaderboard",
                            excluded=False,
                            profile=profile,
                        )
                    )
                else:
                    await conn.execute(
                        update(hl_wallets).where(hl_wallets.c.address == address).values(profile=profile)
                    )
                imported_addresses.add(address)
                imported += 1

            # 全局别名也应用到不在榜单的现有钱包；成功刷新时清理已撤销的旧别名。
            for address, existing in current.items():
                if address in imported_addresses:
                    continue
                profile = dict(existing["profile"] or {})
                profile.pop("leaderboard_account_value", None)
                profile.pop("leaderboard_display_name", None)
                alias = normalized_aliases.get(address)
                if alias:
                    profile["hypurrscan_alias"] = alias
                elif aliases_authoritative:
                    profile.pop("hypurrscan_alias", None)
                if profile != dict(existing["profile"] or {}):
                    await conn.execute(
                        update(hl_wallets).where(hl_wallets.c.address == address).values(profile=profile)
                    )

            stale: list[str] = []
            discovered_cutoff = self.clock() - timedelta(days=30)
            for row in current_rows:
                if row["tier"] != "candidate" or row["excluded"]:
                    continue
                if row["source"] == "leaderboard" and row["address"] not in imported_addresses:
                    if not self._has_protected_metadata(row):
                        stale.append(row["address"])
                    continue
                if row["source"] != "discovered":
                    continue
                if row["address"] in imported_addresses or self._has_protected_metadata(row):
                    continue
                last_trade_value = (row["profile"] or {}).get("last_trade_at")
                try:
                    last_trade_at = datetime.fromisoformat(last_trade_value)
                except (TypeError, ValueError):
                    continue
                if last_trade_at < discovered_cutoff:
                    stale.append(row["address"])
            if stale:
                result = await conn.execute(delete(hl_wallets).where(hl_wallets.c.address.in_(stale)))
                removed = result.rowcount or 0
        return ImportResult(imported, removed)

    async def excluded_addresses(self) -> set[str]:
        async with self.engine.connect() as conn:
            return set(
                (await conn.execute(select(hl_wallets.c.address).where(hl_wallets.c.excluded.is_(True)))).scalars()
            )

    async def has_candidates(self) -> bool:
        async with self.engine.connect() as conn:
            count = await conn.scalar(
                select(func.count()).select_from(hl_wallets).where(
                    hl_wallets.c.tier == "candidate",
                    hl_wallets.c.excluded.is_(False),
                )
            )
        return bool(count)

    async def add(self, address: str) -> bool:
        """手动添加重点钱包；满额时淘汰名义最低的非手动重点。"""
        normalized = normalize_address(address)
        async with self.engine.begin() as conn:
            await self._lock_registry(conn)
            rows = (await conn.execute(select(hl_wallets).with_for_update())).mappings().all()
            existing = next((row for row in rows if row["address"] == normalized), None)
            if existing is not None and existing["excluded"]:
                raise ValueError("该钱包已排除，请先解除排除")
            if (
                existing is not None
                and existing["tier"] in {"focus", "core"}
                and (existing["profile"] or {}).get("manual_focus")
            ):
                return False
            await self._make_focus_room(conn, rows)
            profile = dict(existing["profile"] or {}) if existing is not None else {}
            profile["manual_focus"] = True
            if existing is None:
                await conn.execute(
                    insert(hl_wallets).values(
                        address=normalized,
                        tier="focus",
                        source="manual",
                        excluded=False,
                        profile=profile,
                    )
                )
            else:
                await conn.execute(
                    update(hl_wallets)
                    .where(hl_wallets.c.address == normalized)
                    .values(tier="focus", profile=profile)
                )
        return True

    async def remove(self, address: str) -> bool:
        """移除手动名单关系；配置核心保持不变。"""
        normalized = normalize_address(address)
        if normalized in self.configured_core:
            return False
        async with self.engine.begin() as conn:
            await self._lock_registry(conn)
            row = (
                await conn.execute(select(hl_wallets).where(hl_wallets.c.address == normalized).with_for_update())
            ).mappings().first()
            if row is None:
                return False
            profile = dict(row["profile"] or {})
            if not profile.pop("manual_focus", None):
                return False
            if row["source"] == "manual":
                await conn.execute(delete(hl_wallets).where(hl_wallets.c.address == normalized))
            else:
                await conn.execute(
                    update(hl_wallets)
                    .where(hl_wallets.c.address == normalized)
                    .values(tier="candidate", profile=profile)
                )
        return True

    async def set_core(self, address: str, enabled: bool = True) -> bool:
        """设置或解除手动核心标记。"""
        normalized = normalize_address(address)
        if not enabled and normalized in self.configured_core:
            raise ValueError("配置核心不能解除，请先修改本地配置并重启")
        async with self.engine.begin() as conn:
            await self._lock_registry(conn)
            rows = (await conn.execute(select(hl_wallets).with_for_update())).mappings().all()
            existing = next((row for row in rows if row["address"] == normalized), None)
            if enabled:
                if existing is not None and existing["excluded"]:
                    raise ValueError("已排除钱包不能设为核心，请先解除排除")
                if (
                    existing is not None
                    and existing["tier"] == "core"
                    and (existing["profile"] or {}).get("manual_core")
                ):
                    return False
                core_count = sum(row["tier"] == "core" for row in rows)
                if existing is None or existing["tier"] != "core":
                    if core_count >= self.core_limit:
                        raise ValueError(f"核心钱包最多 {self.core_limit} 个")
                profile = dict(existing["profile"] or {}) if existing is not None else {}
                profile["manual_core"] = True
                if existing is None:
                    await conn.execute(
                        insert(hl_wallets).values(
                            address=normalized,
                            tier="core",
                            source="manual",
                            excluded=False,
                            profile=profile,
                        )
                    )
                else:
                    await conn.execute(
                        update(hl_wallets)
                        .where(hl_wallets.c.address == normalized)
                        .values(tier="core", profile=profile)
                    )
                return True
            if existing is None or existing["tier"] != "core":
                return False
            await self._make_focus_room(conn, rows, excluding=normalized)
            profile = dict(existing["profile"] or {})
            profile.pop("manual_core", None)
            await conn.execute(
                update(hl_wallets)
                .where(hl_wallets.c.address == normalized)
                .values(tier="focus", profile=profile)
            )
            return True

    async def set_excluded(self, address: str, excluded: bool = True) -> bool:
        """设置或解除排除；核心必须先解除。"""
        normalized = normalize_address(address)
        async with self.engine.begin() as conn:
            await self._lock_registry(conn)
            row = (
                await conn.execute(select(hl_wallets).where(hl_wallets.c.address == normalized).with_for_update())
            ).mappings().first()
            if excluded:
                if row is not None and row["tier"] == "core":
                    raise ValueError("核心钱包不能直接排除，请先解除核心")
                if row is not None and row["excluded"]:
                    return False
                if row is None:
                    await conn.execute(
                        insert(hl_wallets).values(
                            address=normalized,
                            tier=None,
                            source="manual",
                            excluded=True,
                        )
                    )
                else:
                    await conn.execute(
                        update(hl_wallets)
                        .where(hl_wallets.c.address == normalized)
                        .values(tier=None, excluded=True)
                    )
                return True
            if row is None or not row["excluded"]:
                return False
            await conn.execute(
                update(hl_wallets)
                .where(hl_wallets.c.address == normalized)
                .values(tier="candidate", excluded=False)
            )
            return True

    async def set_label(self, address: str, note: str | None) -> bool:
        """设置用户备注；空白备注视为清除。"""
        normalized = normalize_address(address)
        normalized_note = note.strip() if isinstance(note, str) else None
        normalized_note = normalized_note or None
        async with self.engine.begin() as conn:
            await self._lock_registry(conn)
            row = (
                await conn.execute(select(hl_wallets).where(hl_wallets.c.address == normalized).with_for_update())
            ).mappings().first()
            if row is None:
                await conn.execute(
                    insert(hl_wallets).values(
                        address=normalized,
                        note=normalized_note,
                        tier="candidate",
                        source="manual",
                        excluded=False,
                    )
                )
                return True
            if row["note"] == normalized_note:
                return False
            await conn.execute(
                update(hl_wallets).where(hl_wallets.c.address == normalized).values(note=normalized_note)
            )
            return True

    async def sync_configured_core(self) -> None:
        """在一个事务中同步配置核心；超限时不产生部分写入。"""
        async with self.engine.begin() as conn:
            await self._lock_registry(conn)
            rows = (await conn.execute(select(hl_wallets).with_for_update())).mappings().all()
            manual_core = {
                row["address"]
                for row in rows
                if row["tier"] == "core"
                and (
                    (row["profile"] or {}).get("manual_core")
                    or not (row["profile"] or {}).get("configured_core")
                )
            }
            combined = manual_core | self.configured_core
            if len(combined) > self.core_limit:
                raise ValueError(f"配置与数据库中的核心钱包合计超过 {self.core_limit} 个")

            by_address = {row["address"]: row for row in rows}
            for row in rows:
                profile = dict(row["profile"] or {})
                if profile.get("configured_core") and row["address"] not in self.configured_core:
                    profile.pop("configured_core", None)
                    if profile.get("manual_core"):
                        tier = "core"
                    elif profile.get("manual_focus"):
                        tier = "focus"
                    else:
                        tier = "candidate"
                    await conn.execute(
                        update(hl_wallets)
                        .where(hl_wallets.c.address == row["address"])
                        .values(tier=tier, profile=profile)
                    )
            for address in self.configured_core:
                existing = by_address.get(address)
                if existing is not None and existing["excluded"]:
                    raise ValueError(f"配置核心钱包已被排除，无法启动：{address}")
                profile = dict(existing["profile"] or {}) if existing is not None else {}
                profile["configured_core"] = True
                if existing is None:
                    await conn.execute(
                        insert(hl_wallets).values(
                            address=address,
                            tier="core",
                            source="manual",
                            excluded=False,
                            profile=profile,
                        )
                    )
                else:
                    await conn.execute(
                        update(hl_wallets)
                        .where(hl_wallets.c.address == address)
                        .values(tier="core", profile=profile)
                    )

    async def list_wallets(
        self,
        *,
        tier: str | None = None,
        excluded: bool | None = None,
    ) -> list[WalletView]:
        """按层级/排除状态查询名单并汇总当前持仓名义。"""
        totals = (
            select(
                hl_positions.c.address,
                func.coalesce(func.sum(func.abs(hl_positions.c.notional_usd)), 0).label("total_notional"),
            )
            .group_by(hl_positions.c.address)
            .subquery()
        )
        stmt = select(hl_wallets, totals.c.total_notional).outerjoin(
            totals, totals.c.address == hl_wallets.c.address
        )
        if tier is not None:
            stmt = stmt.where(hl_wallets.c.tier == tier)
        if excluded is not None:
            stmt = stmt.where(hl_wallets.c.excluded.is_(excluded))
        stmt = stmt.order_by(hl_wallets.c.address)
        async with self.engine.connect() as conn:
            rows = (await conn.execute(stmt)).mappings().all()
        return [
            WalletView(
                address=row["address"],
                note=row["note"],
                tier=row["tier"],
                source=row["source"],
                excluded=row["excluded"],
                style=row["style"],
                profile=dict(row["profile"] or {}),
                total_notional=Decimal(row["total_notional"] or 0),
            )
            for row in rows
        ]

    async def _make_focus_room(
        self,
        conn: AsyncConnection,
        rows: list[Mapping[str, Any]],
        *,
        excluding: str | None = None,
    ) -> None:
        focus_rows = [row for row in rows if row["tier"] == "focus" and row["address"] != excluding]
        if len(focus_rows) < self.focus_limit:
            return
        automatic = [
            row
            for row in focus_rows
            if row["source"] != "manual" and not (row["profile"] or {}).get("manual_focus")
        ]
        if not automatic:
            raise ValueError(f"重点钱包最多 {self.focus_limit} 个，且现有重点均受手动保护")
        addresses = [row["address"] for row in automatic]
        totals = dict(
            (
                await conn.execute(
                    select(
                        hl_positions.c.address,
                        func.coalesce(func.sum(func.abs(hl_positions.c.notional_usd)), 0),
                    )
                    .where(hl_positions.c.address.in_(addresses))
                    .group_by(hl_positions.c.address)
                )
            ).all()
        )
        victim = min(addresses, key=lambda address: (Decimal(totals.get(address, 0)), address))
        await conn.execute(
            update(hl_wallets).where(hl_wallets.c.address == victim).values(tier="candidate")
        )

    @staticmethod
    async def _lock_registry(conn: AsyncConnection) -> None:
        """串行化名单层级写事务，避免并发突破容量上限。"""
        await conn.execute(select(func.pg_advisory_xact_lock(REGISTRY_LOCK_KEY)))

    @staticmethod
    def _has_protected_metadata(
        row: Mapping[str, Any],
        *,
        profile: Mapping[str, Any] | None = None,
    ) -> bool:
        """判断钱包是否含用户字段或画像，自动缓存字段不构成删除保护。"""
        if row["note"] or row["style"] or row["profile_updated_at"] is not None:
            return True
        values = profile if profile is not None else (row["profile"] or {})
        return any(key not in _AUTOMATIC_PROFILE_KEYS for key in values)
