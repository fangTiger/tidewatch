"""Hyperliquid 排行榜的流式读取与候选筛选。"""

from __future__ import annotations

import codecs
import heapq
import json
import re
from collections.abc import AsyncIterable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx


LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
HYPURRSCAN_ALIASES_URL = "https://api.hypurrscan.io/globalAliases"

# 2026-10-06 由官方 ``vaultDetails`` 查询 HLP 父金库得到。
# 构造器允许覆盖，便于上游调整后热更新。
HLP_VAULT_ADDRESS = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"
HLP_CHILD_ADDRESSES = frozenset(
    {
        "0x010461c14e146ac35fe42271bdc1134ee31c703a",
        "0x2e3d94f0562703b25c83308a05046ddaf9a8dd14",
        "0x2ed5c4484ea3ff8b57d5f2fb152a40d9f2b68308",
        "0x31ca8395cf837de08b24da3f660e77761dfb974b",
        "0x469f690213c467c39a23efacfd2816896009d7d8",
        "0x5e177e5e39c0f4e421f5865a6d8beed8d921cb70",
        "0xb0a55f13d22f66e6d495ac98113841b2326e9540",
    }
)
HLP_ADDRESSES = frozenset({HLP_VAULT_ADDRESS, *HLP_CHILD_ADDRESSES})

_ADDRESS_RE = re.compile(r"0x[0-9a-f]{40}")
_ROWS_KEY = '"leaderboardRows"'


@dataclass(frozen=True)
class LeaderboardEntry:
    """一条通过基础校验的排行榜记录。"""

    address: str
    account_value: Decimal
    display_name: str | None


async def iter_leaderboard_rows(
    chunks: AsyncIterable[bytes],
    *,
    max_buffer_chars: int = 1_048_576,
) -> AsyncIterable[dict[str, Any]]:
    """从字节分块增量产出 ``leaderboardRows`` 中的对象。

    缓冲区只保留尚未完成的一条记录；不会构造完整响应字符串或完整 JSON 树。
    """
    decoder = codecs.getincrementaldecoder("utf-8")()
    json_decoder = json.JSONDecoder()
    buffer = ""
    position = 0
    array_started = False
    array_done = False

    async for chunk in chunks:
        if array_done:
            break
        buffer += decoder.decode(chunk)
        while True:
            if not array_started:
                key_at = buffer.find(_ROWS_KEY, position)
                if key_at < 0:
                    if len(buffer) > len(_ROWS_KEY):
                        buffer = buffer[-len(_ROWS_KEY) :]
                    position = 0
                    break
                bracket_at = buffer.find("[", key_at + len(_ROWS_KEY))
                if bracket_at < 0:
                    position = key_at
                    break
                array_started = True
                position = bracket_at + 1

            while position < len(buffer) and buffer[position] in " \t\r\n,":
                position += 1
            if position >= len(buffer):
                buffer = ""
                position = 0
                break
            if buffer[position] == "]":
                array_done = True
                break
            try:
                row, end = json_decoder.raw_decode(buffer, position)
            except json.JSONDecodeError:
                if len(buffer) - position > max_buffer_chars:
                    raise ValueError(f"排行榜单条记录超过 {max_buffer_chars} 字符") from None
                if position:
                    buffer = buffer[position:]
                    position = 0
                break
            if not isinstance(row, dict):
                raise ValueError("排行榜数组元素必须是对象")
            yield row
            position = end
            if position > max_buffer_chars:
                buffer = buffer[position:]
                position = 0

    buffer += decoder.decode(b"", final=True)
    if not array_started:
        raise ValueError("排行榜响应缺少 leaderboardRows 数组")
    if not array_done:
        while position < len(buffer) and buffer[position] in " \t\r\n,":
            position += 1
        if position >= len(buffer) or buffer[position] != "]":
            raise ValueError("排行榜响应在数组结束前中断")


class _TopCandidates:
    """以固定大小最小堆保留排名最高的候选。"""

    def __init__(
        self,
        *,
        excluded_addresses: Iterable[str],
        protocol_addresses: Iterable[str],
        minimum_value: Decimal,
        limit: int,
    ) -> None:
        if limit <= 0:
            raise ValueError("排行榜候选上限必须大于 0")
        self.excluded = {address.lower() for address in excluded_addresses}
        self.protocol = {address.lower() for address in protocol_addresses}
        self.minimum_value = minimum_value
        self.limit = limit
        self._counter = 0
        self._heap: list[tuple[Decimal, int, str, int]] = []
        self._entries: dict[str, tuple[Decimal, int, LeaderboardEntry]] = {}

    def add(self, row: Mapping[str, Any]) -> None:
        address_value = row.get("ethAddress")
        if not isinstance(address_value, str):
            raise ValueError("排行榜记录缺少 ethAddress")
        address = address_value.lower()
        if _ADDRESS_RE.fullmatch(address) is None:
            raise ValueError(f"排行榜地址格式无效：{address_value}")
        try:
            account_value = Decimal(str(row["accountValue"]))
        except (KeyError, InvalidOperation, ValueError) as exc:
            raise ValueError(f"排行榜账户价值无效：{address}") from exc
        if not account_value.is_finite():
            raise ValueError(f"排行榜账户价值无效：{address}")
        if account_value < self.minimum_value or address in self.excluded or address in self.protocol:
            return
        display_name = row.get("displayName")
        if display_name is not None and not isinstance(display_name, str):
            raise ValueError(f"排行榜显示名格式无效：{address}")
        entry = LeaderboardEntry(address, account_value, display_name)
        self._counter += 1
        address_rank = -int(address[2:], 16)
        current = self._entries.get(address)
        if current is not None:
            if (account_value, address_rank) <= (current[0], address_rank):
                return
            self._entries[address] = (account_value, self._counter, entry)
            heapq.heappush(self._heap, (account_value, address_rank, address, self._counter))
            self._compact_if_needed()
            return

        if len(self._entries) >= self.limit:
            self._discard_stale_root()
            if (account_value, address_rank) <= self._heap[0][:2]:
                return
            _, _, victim, _ = heapq.heappop(self._heap)
            self._entries.pop(victim, None)
        self._entries[address] = (account_value, self._counter, entry)
        heapq.heappush(self._heap, (account_value, address_rank, address, self._counter))
        self._compact_if_needed()

    def result(self) -> list[LeaderboardEntry]:
        return sorted(
            (item[-1] for item in self._entries.values()),
            key=lambda entry: (-entry.account_value, entry.address),
        )

    def _discard_stale_root(self) -> None:
        while self._heap:
            _, _, address, counter = self._heap[0]
            current = self._entries.get(address)
            if current is not None and current[1] == counter:
                return
            heapq.heappop(self._heap)

    def _compact_if_needed(self) -> None:
        if len(self._heap) <= self.limit * 2:
            return
        self._heap = [
            (entry.account_value, -int(address[2:], 16), address, counter)
            for address, (_, counter, entry) in self._entries.items()
        ]
        heapq.heapify(self._heap)


def select_top_candidates(
    rows: Iterable[Mapping[str, Any]],
    *,
    excluded_addresses: Iterable[str] = (),
    protocol_addresses: Iterable[str] = HLP_ADDRESSES,
    minimum_value: Decimal = Decimal("1000000"),
    limit: int = 1_000,
) -> list[LeaderboardEntry]:
    """筛选并按账户价值、地址确定性排序排行榜候选。"""
    candidates = _TopCandidates(
        excluded_addresses=excluded_addresses,
        protocol_addresses=protocol_addresses,
        minimum_value=minimum_value,
        limit=limit,
    )
    for row in rows:
        candidates.add(row)
    return candidates.result()


class LeaderboardSource:
    """通过 HTTP 流读取排行榜；HTTP 客户端生命周期由调用方管理。"""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        url: str = LEADERBOARD_URL,
        protocol_addresses: Iterable[str] = HLP_ADDRESSES,
    ) -> None:
        self.client = client
        self.url = url
        self.protocol_addresses = frozenset(address.lower() for address in protocol_addresses)

    async def fetch_top(
        self,
        *,
        excluded_addresses: Iterable[str] = (),
        minimum_value: Decimal = Decimal("1000000"),
        limit: int = 1_000,
    ) -> list[LeaderboardEntry]:
        """流式下载并只保留固定数量的高价值候选。"""
        candidates = _TopCandidates(
            excluded_addresses=excluded_addresses,
            protocol_addresses=self.protocol_addresses,
            minimum_value=minimum_value,
            limit=limit,
        )
        async with self.client.stream("GET", self.url) as response:
            response.raise_for_status()
            async for row in iter_leaderboard_rows(response.aiter_bytes()):
                candidates.add(row)
        return candidates.result()
