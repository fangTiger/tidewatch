"""Hyperliquid 子系统运行时与 M2 名单调度。"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.collectors.base import Backoff, TokenBucket, describe
from tidewatch.collectors.http import make_client
from tidewatch.hyperliquid.client import HyperliquidRestClient
from tidewatch.hyperliquid.importer import AliasCache, LeaderboardImporter
from tidewatch.hyperliquid.leaderboard import HYPURRSCAN_ALIASES_URL, LEADERBOARD_URL, LeaderboardSource
from tidewatch.hyperliquid.monitor import (
    HyperliquidMonitor,
    PriorityReconcileQueue,
    ReconcilePriority,
    ScheduledReconcile,
)
from tidewatch.hyperliquid.registry import WalletRegistry, normalize_address
from tidewatch.hyperliquid.tiers import TierRecalculator
from tidewatch.hyperliquid.ws import HyperliquidWsManager
from tidewatch.store.tables import hl_state


HEALTH_SOURCES = ("hl_rest", "hl_trades_ws", "hl_core_ws", "hl_leaderboard", "hl_profile")
SHANGHAI = ZoneInfo("Asia/Shanghai")
REPO_ROOT = Path(__file__).resolve().parents[2]

logger = logging.getLogger(__name__)


def seconds_until_shanghai_8(now: datetime) -> float:
    """返回从当前时刻到下一个上海时间 08:00 的秒数。"""
    local = now.astimezone(SHANGHAI)
    target = local.replace(hour=8, minute=0, second=0, microsecond=0)
    if target < local:
        target += timedelta(days=1)
    return (target - local).total_seconds()


class HyperliquidRuntime:
    """装配 Hyperliquid 客户端、名单服务和受守护后台任务。"""

    def __init__(
        self,
        settings: Any,
        health: Any,
        *,
        engine: AsyncEngine,
        sink: Any = None,
        proxy_url: str | None = None,
        clock: Clock = utcnow,
        data_dir: Path | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.health = health
        self.engine = engine
        self.sink = sink
        self.clock = clock
        self.monotonic = monotonic
        self.monitoring_bucket = TokenBucket(
            settings.monitor_budget_per_min / 60,
            capacity=settings.monitor_budget_per_min,
        )
        self.batch_bucket = TokenBucket(
            settings.batch_budget_per_min / 60,
            capacity=settings.batch_budget_per_min,
        )
        self.rest = HyperliquidRestClient(
            self.monitoring_bucket,
            self.batch_bucket,
            proxy_mode=settings.proxy,
            proxy_url=proxy_url,
            health=health,
            url=settings.rest_url,
        )
        self.ws = HyperliquidWsManager(
            proxy_mode=settings.proxy,
            proxy_url=proxy_url,
            health=health,
            url=settings.ws_url,
            ping_interval_s=settings.frequencies.ws_ping_interval_s,
        )
        self.http = make_client(settings.proxy, proxy_url, timeout=60)
        self.registry = WalletRegistry(
            engine,
            configured_core=settings.core_wallets,
            core_limit=settings.tiers.core_limit,
            focus_limit=settings.tiers.focus_limit,
            clock=clock,
        )
        leaderboard = LeaderboardSource(
            client=self.http,
            url=getattr(settings, "leaderboard_url", LEADERBOARD_URL),
            protocol_addresses=settings.protocol_excluded_addresses,
        )
        labels = AliasCache(
            (data_dir or REPO_ROOT / "data") / "labels",
            client=self.http,
            url=getattr(settings, "aliases_url", HYPURRSCAN_ALIASES_URL),
            refresh_interval=timedelta(seconds=settings.frequencies.label_refresh_interval_s),
            clock=clock,
        )
        self.importer = LeaderboardImporter(
            self.registry,
            leaderboard,
            labels,
            health=health,
            clock=clock,
            candidate_limit=settings.tiers.candidate_limit,
            minimum_value=Decimal(str(settings.events.discovery_account_value_usd)),
        )
        self.tiers = TierRecalculator(
            engine,
            self.rest,
            minimum_notional=Decimal(str(settings.events.discovery_account_value_usd)),
            focus_limit=settings.tiers.focus_limit,
            clock=clock,
        )
        self.monitor = HyperliquidMonitor(
            settings,
            engine=engine,
            rest=self.rest,
            sink=sink,
            clock=clock,
        )
        self.reconcile_queue = PriorityReconcileQueue()
        self._reconcile_backoffs: dict[str, Backoff] = {}
        self._next_reconcile: dict[str, float] = {}
        self._inflight_reconciles: set[str] = set()
        self._core_addresses: set[str] = set()
        self._ws_configured = False
        self._ws_config_lock = asyncio.Lock()
        self._market_messages: asyncio.Queue[Mapping[str, Any]] = asyncio.Queue(maxsize=1)
        self._core_message_queues: tuple[asyncio.Queue[Mapping[str, Any]], ...] = tuple(
            asyncio.Queue(maxsize=100) for _ in range(settings.tiers.core_limit)
        )
        self._tier_lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._resources_closed = False

    def register_health(self) -> None:
        """登记五个互相独立的健康子源。"""
        stale = self.settings.health
        for name in HEALTH_SOURCES:
            self.health.register(name, getattr(stale, name).stale_after_s)

    async def start(self) -> None:
        """先同步配置核心，再启动全部受关键任务守护的循环。"""
        logger.info(
            "Hyperliquid 使用 %s 代理模式；"
            "上游按出口 IP 限流，切换代理节点会改变限流出口 IP",
            self.settings.proxy,
        )
        await self.registry.sync_configured_core()
        self._stop.clear()
        specs: tuple[tuple[str, Callable[[], Awaitable[None]]], ...] = (
            ("hl-rest", self.run_rest),
            ("hl-trades-ws", self.run_trades_ws),
            ("hl-core-ws", self.run_core_ws),
            ("hl-core-subscriptions", self.run_core_subscriptions),
            ("hl-core-messages", self.run_core_messages),
            ("hl-leaderboard", self.run_leaderboard),
            ("hl-tier-recompute", self.run_tier_recompute),
            ("hl-profile", self.run_profile),
            ("hl-market-data", self.run_market_data),
        )
        self._tasks = [asyncio.create_task(target(), name=name) for name, target in specs]

    async def _idle(self) -> None:
        await self._stop.wait()

    async def _idle_health(self, source: str) -> None:
        """尚未交付的 M4/M5 子源继续沿用 M1 空闲存活上报。"""
        interval = getattr(self.settings.health, source).stale_after_s / 2
        while not self._stop.is_set():
            await self.health.success(source)
            if await self._wait_or_stop(interval):
                return

    async def run_rest(self) -> None:
        """运行重点钱包优先级对账调度。"""
        workers = asyncio.create_task(
            self.run_reconcile_workers(),
            name="hl-reconcile-workers",
        )
        try:
            while not self._stop.is_set():
                if workers.done():
                    await workers
                now = self.monotonic()
                focus = await self.registry.list_wallets(tier="focus", excluded=False)
                if not focus:
                    await self.monitor.run_due_fill_queries()
                    await self._set_health_mode("hl_rest", "degraded")
                    await self.health.success("hl_rest")
                    if await self._wait_or_stop(
                        min(5, self.settings.frequencies.focus_reconcile_interval_s)
                    ):
                        return
                    continue
                await self._set_health_mode("hl_rest", "normal")
                await self.schedule_focus_wallets(now, focus=focus)
                if not self.reconcile_queue and not self._inflight_reconciles:
                    await self.monitor.run_due_fill_queries()
                if await self._wait_or_stop(
                    min(1, self.settings.frequencies.hot_reconcile_interval_s)
                ):
                    return
        finally:
            workers.cancel()
            await asyncio.gather(workers, return_exceptions=True)

    async def run_reconcile_workers(self) -> None:
        """运行有界钱包对账工作池；每次取任务时重新应用优先级。"""
        async with asyncio.TaskGroup() as group:
            for index in range(self.settings.reconcile_concurrency):
                group.create_task(
                    self._run_reconcile_worker(),
                    name=f"hl-reconcile-{index}",
                )

    async def _run_reconcile_worker(self) -> None:
        while not self._stop.is_set():
            now = self.monotonic()
            task = self.reconcile_queue.pop_ready(
                now,
                excluded=self._inflight_reconciles,
            )
            if task is None:
                if await self._wait_or_stop(0.05):
                    return
                continue
            self._inflight_reconciles.add(task.address)
            try:
                await self._execute_reconcile(task, now)
            finally:
                self._inflight_reconciles.discard(task.address)

    async def run_trades_ws(self) -> None:
        await self._idle_health("hl_trades_ws")

    async def run_core_ws(self) -> None:
        """运行核心钱包与 allMids 共用的真实 WS 连接。"""
        while not self._stop.is_set():
            await self._ensure_ws_configured()
            if self._ws_configured:
                await self.ws.run()
                return
            await self._set_health_mode("hl_core_ws", "degraded")
            await self.health.success("hl_core_ws")
            if await self._wait_or_stop(min(5, self.settings.frequencies.core_state_interval_s)):
                return

    async def run_core_subscriptions(self) -> None:
        """轮询数据库名单并动态增减核心钱包订阅。"""
        while not self._stop.is_set():
            await self.sync_core_subscriptions()
            if not self._core_addresses:
                await self.health.success("hl_core_ws")
            if await self._wait_or_stop(self.settings.frequencies.core_state_interval_s):
                return

    async def run_leaderboard(self) -> None:
        """空候选池立即导入；其后每 12 小时导入并紧接一次分层重算。"""
        initial_delay = await self._leaderboard_initial_delay()
        if initial_delay > 0 and await self._wait_or_stop(initial_delay):
            return
        while not self._stop.is_set():
            try:
                await self.importer.import_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Hyperliquid 排行榜导入失败")
                retry_delay = min(300, self.settings.frequencies.leaderboard_interval_s)
                if await self._wait_or_stop(retry_delay):
                    return
                continue
            try:
                async with self._tier_lock:
                    await self.tiers.recompute()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Hyperliquid 排行榜导入后的分层重算失败")
                retry_delay = min(300, self.settings.frequencies.leaderboard_interval_s)
                if await self._wait_or_stop(retry_delay):
                    return
                try:
                    async with self._tier_lock:
                        await self.tiers.recompute()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Hyperliquid 导入后的分层重算重试失败")
            if await self._wait_or_stop(self.settings.frequencies.leaderboard_interval_s):
                return

    async def run_tier_recompute(self) -> None:
        """每天上海时间 08:00 运行与导入后相同的分层规则。"""
        last_run_date = None
        retrying = False
        while not self._stop.is_set():
            now = self.clock()
            if retrying:
                delay = min(300, self.settings.frequencies.tier_recompute_interval_s)
            else:
                delay = seconds_until_shanghai_8(now)
                if delay == 0 and last_run_date == now.astimezone(SHANGHAI).date():
                    delay = self.settings.frequencies.tier_recompute_interval_s
            if await self._wait_or_stop(delay):
                return
            try:
                async with self._tier_lock:
                    await self.tiers.recompute()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Hyperliquid 每日分层重算失败")
                retrying = True
            else:
                retrying = False
                last_run_date = self.clock().astimezone(SHANGHAI).date()

    async def _leaderboard_initial_delay(self) -> float:
        """按上次完成游标恢复十二小时节拍；空候选池仍立即导入。"""
        if not await self.registry.has_candidates():
            return 0
        async with self.engine.connect() as conn:
            cursor = await conn.scalar(
                select(hl_state.c.cursor).where(hl_state.c.name == "leaderboard_import")
            )
        if not isinstance(cursor, dict):
            return self.settings.frequencies.leaderboard_interval_s
        if cursor.get("phase") != "complete":
            return 0
        try:
            completed_at = datetime.fromisoformat(cursor["completed_at"])
            elapsed = (self.clock() - completed_at).total_seconds()
        except (KeyError, TypeError, ValueError):
            return self.settings.frequencies.leaderboard_interval_s
        return max(0, self.settings.frequencies.leaderboard_interval_s - max(0, elapsed))

    async def run_profile(self) -> None:
        await self._idle_health("hl_profile")

    async def run_market_data(self) -> None:
        """消费 allMids 最新消息；满队列时只保留最新价格。"""
        while not self._stop.is_set():
            getter = asyncio.create_task(self._market_messages.get())
            stopper = asyncio.create_task(self._stop.wait())
            done, pending = await asyncio.wait((getter, stopper), return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if stopper in done:
                return
            mids = await getter
            await self.monitor.update_mids(mids)

    async def run_core_messages(self) -> None:
        """按钱包保序、钱包间并行处理核心状态，避免阻塞 WS 读取。"""
        async with asyncio.TaskGroup() as group:
            for index, queue in enumerate(self._core_message_queues):
                group.create_task(
                    self._run_core_message_worker(queue),
                    name=f"hl-core-message-{index}",
                )

    async def _run_core_message_worker(
        self,
        queue: asyncio.Queue[Mapping[str, Any]],
    ) -> None:
        while True:
            if self._stop.is_set():
                try:
                    message = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
            else:
                getter = asyncio.create_task(queue.get())
                stopper = asyncio.create_task(self._stop.wait())
                done, pending = await asyncio.wait(
                    (getter, stopper), return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                if stopper in done and getter not in done:
                    continue
                message = await getter
            data = message.get("data")
            try:
                await self.monitor.handle_core_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.health.failure("hl_core_ws", describe(exc))
                raise

    @property
    def core_addresses(self) -> set[str]:
        return set(self._core_addresses)

    def trigger_trade(self, address: str) -> None:
        """供 M4 成交流调用：按配置去抖后安排最高优先级对账。"""
        self.reconcile_queue.schedule(
            address,
            ReconcilePriority.TRADE,
            due_at=self.monotonic() + self.settings.frequencies.trade_debounce_s,
            reason="成交触发",
        )

    async def schedule_focus_wallets(
        self,
        now: float,
        *,
        focus: list[Any] | None = None,
    ) -> None:
        """把到期的重点钱包按热点等级加入队列，不制造重复任务。"""
        wallets = focus if focus is not None else await self.registry.list_wallets(tier="focus", excluded=False)
        hot = {item.address: item for item in await self.monitor.hot_wallets()}
        active = {wallet.address for wallet in wallets}
        for address in list(self._next_reconcile):
            if address not in active:
                self._next_reconcile.pop(address, None)
        distance_limit = Decimal(str(self.settings.events.hot_liquidation_distance_ratio))
        for wallet in wallets:
            if (
                wallet.address in self.reconcile_queue
                or wallet.address in self._inflight_reconciles
            ):
                continue
            if self._next_reconcile.get(wallet.address, now) > now:
                continue
            item = hot.get(wallet.address)
            if item is None:
                priority = ReconcilePriority.NORMAL
                reason = "普通对账"
            elif item.liquidation_distance is not None and item.liquidation_distance <= distance_limit:
                priority = ReconcilePriority.LIQUIDATION
                reason = "接近强平钱包对账"
            else:
                priority = ReconcilePriority.HOT
                reason = "热点钱包对账"
            self.reconcile_queue.schedule(wallet.address, priority, due_at=now, reason=reason)

    async def run_reconcile_once(self, now: float) -> bool:
        """处理一项到期任务；预算不足时短暂顺延并保留原优先级。"""
        task = self.reconcile_queue.pop_ready(now, excluded=self._inflight_reconciles)
        if task is None:
            return False
        self._inflight_reconciles.add(task.address)
        try:
            await self._execute_reconcile(task, now)
        finally:
            self._inflight_reconciles.discard(task.address)
        return True

    async def _execute_reconcile(self, task: ScheduledReconcile, now: float) -> None:
        """执行单钱包任务并按结果完成、顺延或退避。"""
        try:
            complete = await self.monitor.reconcile(task.address)
        except asyncio.CancelledError:
            self.reconcile_queue.defer(task, due_at=now)
            raise
        except Exception as exc:
            backoff = self._reconcile_backoffs.setdefault(task.address, Backoff())
            delay = backoff.next()
            logger.exception(
                "Hyperliquid 钱包 %s 对账失败，优先级 %s，%g 秒后重试",
                _short_address(task.address),
                task.priority.name,
                delay,
            )
            await self.health.failure("hl_rest", describe(exc))
            self.reconcile_queue.defer(task, due_at=now + delay)
            return
        if not complete:
            self.reconcile_queue.defer(task, due_at=now + 0.1)
            return
        interval = await self._interval_after(task)
        self._reconcile_backoffs.pop(task.address, None)
        self._next_reconcile[task.address] = self.monotonic() + interval

    async def sync_core_subscriptions(self) -> None:
        """把数据库核心名单与 WS 用户订阅同步。"""
        await self._ensure_ws_configured()
        wallets = await self.registry.list_wallets(tier="core", excluded=False)
        desired = {wallet.address for wallet in wallets}
        if desired and not self._ws_configured:
            await self._ensure_ws_configured(force=True)
        if not self._ws_configured:
            self._core_addresses = desired
            await self._set_health_mode("hl_core_ws", "degraded")
            return
        for address in sorted(desired - self._core_addresses):
            await self.ws.subscribe("core", {"type": "userFills", "user": address})
            await self.ws.subscribe(
                "core",
                {"type": "allDexsClearinghouseState", "user": address},
            )
        for address in sorted(self._core_addresses - desired):
            await self.ws.unsubscribe("core", {"type": "userFills", "user": address})
            await self.ws.unsubscribe(
                "core",
                {"type": "allDexsClearinghouseState", "user": address},
            )
        self._core_addresses = desired
        await self._set_health_mode("hl_core_ws", "normal" if desired else "degraded")

    async def _ensure_ws_configured(self, *, force: bool = False) -> None:
        if self._ws_configured:
            return
        async with self._ws_config_lock:
            if self._ws_configured:
                return
            core = await self.registry.list_wallets(tier="core", excluded=False)
            focus = await self.registry.list_wallets(tier="focus", excluded=False)
            if not force and not core and not focus:
                return
            self.ws.add_connection(
                "core",
                handler=self._handle_ws_message,
                on_connect=self._on_core_connected,
                health_source="hl_core_ws",
            )
            self.ws.add_subscription("core", {"type": "allMids"})
            for wallet in core:
                self.ws.add_subscription("core", {"type": "userFills", "user": wallet.address})
                self.ws.add_subscription(
                    "core",
                    {"type": "allDexsClearinghouseState", "user": wallet.address},
                )
            self._core_addresses = {wallet.address for wallet in core}
            self._ws_configured = True
            await self._set_health_mode("hl_core_ws", "normal" if core else "degraded")

    async def _handle_ws_message(self, message: Any) -> bool:
        if not isinstance(message, Mapping):
            return False
        channel = message.get("channel")
        if channel in {"pong", "subscriptionResponse"}:
            return True
        if channel == "allMids":
            data = message.get("data")
            mids = data.get("mids") if isinstance(data, Mapping) and isinstance(data.get("mids"), Mapping) else data
            if isinstance(mids, Mapping):
                if self._market_messages.full():
                    self._market_messages.get_nowait()
                self._market_messages.put_nowait(dict(mids))
                return True
            return False
        if channel in {"userFills", "allDexsClearinghouseState"}:
            data = message.get("data")
            if not isinstance(data, Mapping) or not isinstance(data.get("user"), str):
                return False
            if channel == "userFills" and not isinstance(data.get("fills"), list):
                return False
            if channel == "allDexsClearinghouseState" and not (
                isinstance(data.get("clearinghouseStates"), (list, dict))
                or isinstance(data.get("states"), (list, dict))
                or isinstance(data.get("assetPositions"), list)
            ):
                return False
            address = normalize_address(data["user"])
            index = int(address, 16) % len(self._core_message_queues)
            await self._core_message_queues[index].put(dict(message))
            return True
        return False

    async def _on_core_connected(self) -> None:
        """首次连接及每次重连后，立即把全部热点钱包提到队首。"""
        now = self.monotonic()
        distance_limit = Decimal(str(self.settings.events.hot_liquidation_distance_ratio))
        for item in await self.monitor.hot_wallets():
            priority = (
                ReconcilePriority.LIQUIDATION
                if item.liquidation_distance is not None
                and item.liquidation_distance <= distance_limit
                else ReconcilePriority.HOT
            )
            self.reconcile_queue.schedule(
                item.address,
                priority,
                due_at=now,
                reason="核心 WS 重连热点对账",
            )

    async def _interval_after(self, task: ScheduledReconcile) -> float:
        hot = {item.address for item in await self.monitor.hot_wallets()}
        if task.address in hot:
            return self.settings.frequencies.hot_reconcile_interval_s
        return self.settings.frequencies.focus_reconcile_interval_s

    async def _set_health_mode(self, source: str, mode: str) -> None:
        setter = getattr(self.health, "set_mode", None)
        if setter is not None:
            await setter(source, mode)

    async def _wait_or_stop(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return True

    def background_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(self._tasks)

    async def stop(self) -> None:
        """停止新接收并立即取消在途后台请求，避免等待 HTTP 超时。"""
        current = asyncio.current_task()
        managed = [task for task in self._tasks if task is not current]
        for task in managed:
            if not task.done():
                task.cancel()
        self._stop.set()
        await self.ws.aclose()
        if managed:
            await asyncio.gather(*managed, return_exceptions=True)
        await self.close_resources()

    async def close_resources(self) -> None:
        """关闭 HTTP 资源；对测试与部分启动可重复调用。"""
        if self._resources_closed:
            return
        self._resources_closed = True
        await self.rest.aclose()
        await self.http.aclose()


def _short_address(address: str) -> str:
    return f"{address[:6]}…{address[-4:]}"
