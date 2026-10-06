"""排行榜名单导入、Hypurrscan 标签缓存与导入游标。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tidewatch.clock import Clock, utcnow
from tidewatch.hyperliquid.logging import exception_summary
from tidewatch.hyperliquid.leaderboard import HYPURRSCAN_ALIASES_URL
from tidewatch.hyperliquid.registry import ImportResult, WalletRegistry, normalize_address
from tidewatch.store.tables import hl_state


ALIAS_CACHE_NAME = "hypurrscan-global-aliases.json"


@dataclass(frozen=True)
class AliasResult:
    """一次标签读取结果；刷新失败时 aliases 仍可能来自旧缓存。"""

    aliases: dict[str, str]
    refresh_failed: bool
    error: str | None


class AliasCache:
    """每天刷新 Hypurrscan 全局别名，失败时返回最后一次成功缓存。"""

    def __init__(
        self,
        directory: Path,
        *,
        client: httpx.AsyncClient,
        url: str = HYPURRSCAN_ALIASES_URL,
        refresh_interval: timedelta = timedelta(days=1),
        clock: Clock = utcnow,
    ) -> None:
        self.directory = directory
        self.client = client
        self.url = url
        self.refresh_interval = refresh_interval
        self.clock = clock

    @property
    def path(self) -> Path:
        return self.directory / ALIAS_CACHE_NAME

    async def load(self) -> AliasResult:
        cached_at, cached = self._read_cache()
        now = self.clock()
        if cached_at is not None and now - cached_at < self.refresh_interval:
            return AliasResult(cached, False, None)
        try:
            response = await self.client.get(self.url)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Hypurrscan 别名响应必须是对象")
            aliases = self._normalize_aliases(payload)
            self._write_cache(now, aliases)
            return AliasResult(aliases, False, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return AliasResult(cached, True, exception_summary(exc))

    def _read_cache(self) -> tuple[datetime | None, dict[str, str]]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            fetched_at = datetime.fromisoformat(payload["fetched_at"])
            aliases = self._normalize_aliases(payload["aliases"])
            return fetched_at, aliases
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            return None, {}

    @staticmethod
    def _normalize_aliases(payload: dict[Any, Any]) -> dict[str, str]:
        aliases: dict[str, str] = {}
        for address, label in payload.items():
            if not isinstance(address, str) or not isinstance(label, str) or not label:
                continue
            try:
                aliases[normalize_address(address)] = label
            except ValueError:
                continue
        return aliases

    def _write_cache(self, fetched_at: datetime, aliases: dict[str, str]) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(
                {"fetched_at": fetched_at.isoformat(), "aliases": aliases},
                ensure_ascii=False,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        temporary.replace(self.path)


class LeaderboardImporter:
    """把流式排行榜与标签合并进名单事实源。"""

    def __init__(
        self,
        registry: WalletRegistry,
        source: Any,
        aliases: Any,
        *,
        health: Any = None,
        clock: Clock = utcnow,
        health_source: str = "hl_leaderboard",
        candidate_limit: int = 1_000,
        minimum_value: Decimal = Decimal("1000000"),
    ) -> None:
        self.registry = registry
        self.source = source
        self.aliases = aliases
        self.health = health
        self.clock = clock
        self.health_source = health_source
        self.candidate_limit = candidate_limit
        self.minimum_value = minimum_value

    async def import_once(self) -> ImportResult:
        """执行一次导入；排行榜失败不改名单，标签失败则用旧缓存继续。"""
        try:
            await self._write_progress(
                {"phase": "downloading", "started_at": self.clock().isoformat()}
            )
            excluded = await self.registry.excluded_addresses()
            entries = await self.source.fetch_top(
                excluded_addresses=excluded,
                limit=self.candidate_limit,
                minimum_value=self.minimum_value,
            )
            await self._write_progress(
                {
                    "phase": "merging",
                    "started_at": self.clock().isoformat(),
                    "candidate_count": len(entries),
                }
            )
            alias_result: AliasResult = await self.aliases.load()
            result = await self.registry.import_leaderboard(
                entries,
                aliases=alias_result.aliases,
                aliases_authoritative=not alias_result.refresh_failed,
            )
            await self._write_cursor(result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._failure(exception_summary(exc))
            raise
        if alias_result.refresh_failed:
            await self._failure(alias_result.error or "Hypurrscan 别名刷新失败")
        else:
            await self._success()
        return result

    async def _write_cursor(self, result: ImportResult) -> None:
        now = self.clock()
        cursor = {
            "phase": "complete",
            "completed_at": now.isoformat(),
            "imported": result.imported,
            "removed": result.removed,
        }
        await self._write_progress(cursor)

    async def _write_progress(self, cursor: dict[str, Any]) -> None:
        now = self.clock()
        stmt = pg_insert(hl_state).values(name="leaderboard_import", cursor=cursor, updated_at=now)
        stmt = stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
        )
        async with self.registry.engine.begin() as conn:
            await conn.execute(stmt)

    async def _success(self) -> None:
        if self.health is not None:
            await self.health.success(self.health_source)

    async def _failure(self, detail: str) -> None:
        if self.health is not None:
            await self.health.failure(self.health_source, detail)
