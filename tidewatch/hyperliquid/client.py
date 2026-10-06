"""Hyperliquid REST ``POST /info`` 客户端。"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

import httpx

from tidewatch.collectors.base import Backoff, TokenBucket
from tidewatch.collectors.http import make_client
from tidewatch.hyperliquid.logging import exception_summary

logger = logging.getLogger(__name__)

REST_URL = "https://api.hyperliquid.xyz/info"

Budget = Literal["monitor", "batch"]
Sleep = Callable[[float], Awaitable[None]]
T = TypeVar("T")

# 系统设计 7.1 明确列出的基础权重；官方未单列的 info 请求按通用权重 20 处理。
BASE_WEIGHTS: dict[str, int] = {
    "clearinghouseState": 2,
    "spotClearinghouseState": 2,
    "userFills": 20,
    "userFillsByTime": 20,
    "portfolio": 20,
    "userAbstraction": 20,
    "userTwapSliceFills": 20,
}
VOLUME_WEIGHT_TYPES = frozenset({"userFills", "userFillsByTime", "userTwapSliceFills"})
MAX_BASE_WEIGHT = max(BASE_WEIGHTS.values())


class HyperliquidRateLimitError(httpx.HTTPStatusError):
    """上游明确限流；由共享熔断器处理，不进入单钱包退避。"""


@dataclass(frozen=True)
class _RequestPermit:
    probe: bool


@dataclass(frozen=True)
class _LimitState:
    delay: float
    recent_count: int
    first_in_period: bool


class _GlobalRateController:
    """所有 Hyperliquid REST 请求共享的冷却门与 AIMD 状态。"""

    def __init__(
        self,
        monitoring_bucket: Any,
        batch_bucket: Any,
        *,
        monotonic: Callable[[], float],
        sleep: Sleep,
        burst_window_s: float,
        aimd_min_budget_per_min: float,
        aimd_recovery_interval_s: float,
    ) -> None:
        self._buckets = {
            "monitor": monitoring_bucket,
            "batch": batch_bucket,
        }
        self._targets = {
            name: getattr(bucket, "rate_per_s", 0.0) * 60
            for name, bucket in self._buckets.items()
        }
        self._effective = dict(self._targets)
        self._monotonic = monotonic
        self._sleep = sleep
        self._burst_window_s = burst_window_s
        self._minimum = aimd_min_budget_per_min
        self._recovery_interval_s = aimd_recovery_interval_s
        self._backoff = Backoff()
        self._cooling = False
        self._cooldown_until = 0.0
        self._probe_in_flight = False
        self._next_recovery_at: float | None = None
        self._recent_limits: deque[float] = deque()
        self._lock = asyncio.Lock()
        self._changed = asyncio.Event()

    @property
    def cooldown_until(self) -> float:
        return self._cooldown_until

    async def try_start(self) -> _RequestPermit | None:
        async with self._lock:
            await self._recover_locked(self._monotonic())
            return self._start_locked(self._monotonic())

    async def start(self) -> _RequestPermit:
        while True:
            delay = 0.0
            event: asyncio.Event | None = None
            async with self._lock:
                now = self._monotonic()
                await self._recover_locked(now)
                permit = self._start_locked(now)
                if permit is not None:
                    return permit
                if now < self._cooldown_until:
                    delay = self._cooldown_until - now
                else:
                    event = self._changed
            if delay > 0:
                await self._sleep(delay)
            elif event is not None:
                await event.wait()

    def _start_locked(self, now: float) -> _RequestPermit | None:
        if not self._cooling:
            return _RequestPermit(probe=False)
        if now < self._cooldown_until or self._probe_in_flight:
            return None
        self._probe_in_flight = True
        return _RequestPermit(probe=True)

    async def validate(self, permit: _RequestPermit) -> bool:
        async with self._lock:
            if permit.probe:
                return self._probe_in_flight
            return not self._cooling

    async def cancel(self, permit: _RequestPermit) -> None:
        if not permit.probe:
            return
        async with self._lock:
            if self._probe_in_flight:
                self._probe_in_flight = False
                self._notify_locked()

    async def success(self, permit: _RequestPermit) -> bool:
        """记录请求成功；冷却前已在途的成功不能提前恢复健康状态。"""
        async with self._lock:
            if permit.probe:
                self._cooling = False
                self._cooldown_until = 0.0
                self._probe_in_flight = False
                self._backoff.reset()
                self._notify_locked()
                return True
            if not self._cooling:
                self._backoff.reset()
                return True
            return False

    async def other_failure(self, permit: _RequestPermit) -> float:
        """探测失败时继续指数冷却；普通在途失败不影响共享门。"""
        if not permit.probe:
            return 0.0
        async with self._lock:
            now = self._monotonic()
            delay = self._backoff.next()
            self._cooling = True
            self._cooldown_until = now + delay
            self._probe_in_flight = False
            self._notify_locked()
            return delay

    async def limited(self, permit: _RequestPermit) -> _LimitState:
        async with self._lock:
            now = self._monotonic()
            self._recent_limits.append(now)
            while self._recent_limits and self._recent_limits[0] < now - 60:
                self._recent_limits.popleft()
            self._next_recovery_at = now + self._recovery_interval_s
            first = not self._cooling or permit.probe
            if first:
                delay = self._backoff.next()
                self._cooling = True
                self._cooldown_until = now + delay
                await self._decrease_rates_locked()
            else:
                delay = max(0.0, self._cooldown_until - now)
            if permit.probe:
                self._probe_in_flight = False
            self._notify_locked()
            return _LimitState(delay, len(self._recent_limits), first)

    async def _decrease_rates_locked(self) -> None:
        for name, bucket in self._buckets.items():
            target = self._targets[name]
            if target <= 0 or not hasattr(bucket, "set_rate"):
                continue
            minimum = min(self._minimum, target)
            new_rate = max(minimum, self._effective[name] * 0.5)
            await self._set_rate_locked(name, new_rate)

    async def _recover_locked(self, now: float) -> None:
        if self._next_recovery_at is None or now < self._next_recovery_at:
            return
        intervals = int((now - self._next_recovery_at) // self._recovery_interval_s) + 1
        self._next_recovery_at += intervals * self._recovery_interval_s
        for name, bucket in self._buckets.items():
            target = self._targets[name]
            if target <= 0 or not hasattr(bucket, "set_rate"):
                continue
            new_rate = min(target, self._effective[name] + target * 0.1 * intervals)
            await self._set_rate_locked(name, new_rate)
        if all(self._effective[name] >= target for name, target in self._targets.items()):
            self._next_recovery_at = None

    async def _set_rate_locked(self, name: str, rate_per_min: float) -> None:
        old_rate = self._effective[name]
        if rate_per_min == old_rate:
            return
        self._effective[name] = rate_per_min
        capacity = max(MAX_BASE_WEIGHT, rate_per_min / 60 * self._burst_window_s)
        await self._buckets[name].set_rate(rate_per_min / 60, capacity=capacity)
        logger.info(
            "Hyperliquid %s REST 有效速率由 %.1f 调整为 %.1f 权重/分钟，桶容量 %.1f",
            "监控" if name == "monitor" else "批处理",
            old_rate,
            rate_per_min,
            capacity,
        )

    def _notify_locked(self) -> None:
        self._changed.set()
        self._changed = asyncio.Event()


class HyperliquidRestClient:
    """按监控/批处理独立预算调用 Hyperliquid ``/info``。"""

    def __init__(
        self,
        monitoring_bucket: TokenBucket,
        batch_bucket: TokenBucket,
        *,
        proxy_mode: str = "global",
        proxy_url: str | None = None,
        health: Any = None,
        client: httpx.AsyncClient | None = None,
        sleep: Sleep = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        url: str = REST_URL,
        health_source: str = "hl_rest",
        burst_window_s: float = 10,
        aimd_min_budget_per_min: float = 120,
        aimd_recovery_interval_s: float = 60,
    ) -> None:
        if proxy_mode not in {"global", "direct"}:
            raise ValueError("Hyperliquid 代理模式必须是 global 或 direct")
        self.monitoring_bucket = monitoring_bucket
        self.batch_bucket = batch_bucket
        self.proxy_mode = proxy_mode
        self.health = health
        self.url = url
        self.health_source = health_source
        self._sleep = sleep
        self._monotonic = monotonic
        self._client = client or make_client(proxy_mode, proxy_url)
        self._rate_controller = _GlobalRateController(
            monitoring_bucket,
            batch_bucket,
            monotonic=monotonic,
            sleep=sleep,
            burst_window_s=burst_window_s,
            aimd_min_budget_per_min=aimd_min_budget_per_min,
            aimd_recovery_interval_s=aimd_recovery_interval_s,
        )

    @property
    def cooldown_until(self) -> float:
        """当前共享冷却结束的单调时钟时间。"""
        return self._rate_controller.cooldown_until

    async def info(self, request_type: str, *, budget: Budget = "monitor", **params: Any) -> Any:
        """发送一条 info 请求；JSON 中的数字字符串保持上游原样。"""
        return await self.request({"type": request_type, **params}, budget=budget)

    async def try_info(
        self,
        request_type: str,
        *,
        budget: Budget = "monitor",
        **params: Any,
    ) -> tuple[bool, Any]:
        """预算足够时立即发送请求；不足时返回 ``(False, None)`` 供优先队列顺延。"""
        bucket = self._bucket(budget)
        weight = BASE_WEIGHTS.get(request_type, 20)
        try_acquire = getattr(bucket, "try_acquire", None)
        if try_acquire is None:
            await bucket.acquire(weight)
        elif not await try_acquire(weight):
            return False, None
        permit = await self._rate_controller.try_start()
        if permit is None:
            await self._refund_weight(bucket, weight)
            return False, None
        try:
            valid = await self._rate_controller.validate(permit)
        except BaseException:
            await self._finish_uninterruptibly(
                self._abandon_permit(bucket, weight, permit)
            )
            raise
        if not valid:
            await self._finish_uninterruptibly(
                self._abandon_permit(bucket, weight, permit)
            )
            return False, None
        try:
            data = await self.request(
                {"type": request_type, **params},
                budget=budget,
                initial_weight_acquired=True,
                initial_permit=permit,
                retry_transient=False,
            )
        except HyperliquidRateLimitError:
            return False, None
        return True, data

    async def request(
        self,
        payload: Mapping[str, Any],
        *,
        budget: Budget = "monitor",
        initial_weight_acquired: bool = False,
        initial_permit: _RequestPermit | None = None,
        retry_transient: bool = True,
    ) -> Any:
        request_type = payload.get("type")
        if not isinstance(request_type, str) or not request_type:
            raise ValueError("Hyperliquid info 请求必须包含非空 type")
        bucket = self._bucket(budget)
        base_weight = BASE_WEIGHTS.get(request_type, 20)
        backoff = Backoff()

        first_attempt = True
        while True:
            if first_attempt and initial_permit is not None:
                permit = initial_permit
            elif not (first_attempt and initial_weight_acquired):
                try:
                    await bucket.acquire(base_weight)
                except asyncio.CancelledError:
                    raise
                permit = None
                try:
                    permit = await self._rate_controller.start()
                except BaseException:
                    await self._refund_weight(bucket, base_weight)
                    raise
                try:
                    valid = await self._rate_controller.validate(permit)
                except BaseException:
                    await self._finish_uninterruptibly(
                        self._abandon_permit(bucket, base_weight, permit)
                    )
                    raise
                if not valid:
                    await self._finish_uninterruptibly(
                        self._abandon_permit(bucket, base_weight, permit)
                    )
                    continue
            else:
                permit = await self._rate_controller.start()
            first_attempt = False
            try:
                response = await self._client.post(self.url, json=dict(payload))
            except asyncio.CancelledError:
                await self._finish_uninterruptibly(
                    self._rate_controller.other_failure(permit)
                )
                raise
            except httpx.RequestError as exc:
                await self._finish_uninterruptibly(
                    self._rate_controller.other_failure(permit)
                )
                await self._report_failure(exc)
                if not retry_transient:
                    raise
                delay = backoff.next()
                logger.warning(
                    "子源 %s 请求 %s（权重 %d）网络失败：%s；%g 秒后重试，代理模式 %s",
                    self.health_source,
                    request_type,
                    base_weight,
                    exception_summary(exc),
                    delay,
                    self.proxy_mode,
                )
                await self._sleep(delay)
                continue

            if self._is_rate_limited(response):
                error = httpx.HTTPStatusError(
                    f"Hyperliquid 限流响应 HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                state = await self._finish_uninterruptibly(
                    self._rate_controller.limited(permit)
                )
                if state.first_in_period:
                    await self._report_failure(error)
                    logger.warning(
                        "子源 %s 请求 %s（权重 %d）触发全局限流，冷却 %g 秒，"
                        "最近 1 分钟 %d 次 429，代理模式 %s",
                        self.health_source,
                        request_type,
                        base_weight,
                        state.delay,
                        state.recent_count,
                        self.proxy_mode,
                    )
                else:
                    logger.debug(
                        "子源 %s 请求 %s（权重 %d）在同一冷却期再次限流，"
                        "剩余冷却 %g 秒，最近 1 分钟 %d 次 429，代理模式 %s",
                        self.health_source,
                        request_type,
                        base_weight,
                        state.delay,
                        state.recent_count,
                        self.proxy_mode,
                    )
                if not retry_transient:
                    raise HyperliquidRateLimitError(
                        str(error), request=response.request, response=response
                    ) from error
                continue

            if response.status_code >= 500:
                await self._finish_uninterruptibly(
                    self._rate_controller.other_failure(permit)
                )
                error = httpx.HTTPStatusError(
                    f"Hyperliquid HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                await self._report_failure(error)
                if not retry_transient:
                    raise error
                delay = backoff.next()
                logger.warning(
                    "子源 %s 请求 %s（权重 %d）服务端错误 HTTP %d；"
                    "%g 秒后重试，代理模式 %s",
                    self.health_source,
                    request_type,
                    base_weight,
                    response.status_code,
                    delay,
                    self.proxy_mode,
                )
                await self._sleep(delay)
                continue

            try:
                response.raise_for_status()
                data = response.json()
            except asyncio.CancelledError:
                await self._finish_uninterruptibly(
                    self._rate_controller.other_failure(permit)
                )
                raise
            except Exception as exc:
                await self._finish_uninterruptibly(
                    self._rate_controller.other_failure(permit)
                )
                await self._report_failure(exc)
                raise

            backoff.reset()
            recovered = await self._finish_uninterruptibly(
                self._rate_controller.success(permit)
            )
            extra_weight = self._response_weight(request_type, data)
            if extra_weight:
                await self._acquire_weight(bucket, extra_weight)
            if recovered:
                await self._report_success()
            return data

    @staticmethod
    def _is_rate_limited(response: httpx.Response) -> bool:
        if response.status_code == 429:
            return True
        try:
            data = response.json()
        except Exception:
            text = response.text
        else:
            if isinstance(data, str):
                text = data
            elif isinstance(data, Mapping):
                text = " ".join(
                    json.dumps(data.get(key, ""), ensure_ascii=False)
                    for key in ("error", "message", "status", "response")
                )
            else:
                return False
        normalized = text.casefold()
        return any(
            marker in normalized
            for marker in ("rate limit", "too many request", "限流", "http 429")
        )

    @staticmethod
    async def _acquire_weight(bucket: Any, weight: int) -> None:
        """追加权重大于动态桶容量时分段扣减，避免合法大响应失败。"""
        initial_capacity = getattr(bucket, "capacity", None)
        if not isinstance(initial_capacity, (int, float)) or initial_capacity <= 0:
            await bucket.acquire(weight)
            return
        remaining = float(weight)
        while remaining > 1e-9:
            capacity = getattr(bucket, "capacity", MAX_BASE_WEIGHT)
            chunk = min(remaining, capacity, MAX_BASE_WEIGHT)
            await bucket.acquire(chunk)
            remaining -= chunk

    @staticmethod
    async def _refund_weight(bucket: Any, weight: int) -> None:
        """请求尚未发送时归还已取得的预算；测试桶可不实现归还。"""
        refund = getattr(bucket, "refund", None)
        if refund is not None:
            await refund(weight)

    async def _abandon_permit(
        self,
        bucket: Any,
        weight: int,
        permit: _RequestPermit,
    ) -> None:
        """请求发出前放弃许可，并归还已经预留的预算。"""
        await self._refund_weight(bucket, weight)
        await self._rate_controller.cancel(permit)

    @staticmethod
    async def _finish_uninterruptibly(awaitable: Awaitable[T]) -> T:
        """即使调用方被取消，也先完成共享限流状态的终态转换。"""
        task = asyncio.create_task(awaitable)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await task
            finally:
                raise

    def _bucket(self, budget: Budget) -> TokenBucket:
        if budget == "monitor":
            return self.monitoring_bucket
        if budget == "batch":
            return self.batch_bucket
        raise ValueError("Hyperliquid REST 预算必须是 monitor 或 batch")

    @staticmethod
    def _response_weight(request_type: str, data: Any) -> int:
        if request_type not in VOLUME_WEIGHT_TYPES or not isinstance(data, list):
            return 0
        return len(data) // 20

    async def _report_success(self) -> None:
        if self.health is None:
            return
        try:
            await self.health.success(self.health_source)
        except Exception as exc:
            logger.error(
                "子源 %s 健康成功状态上报失败：%s",
                self.health_source,
                exception_summary(exc),
            )

    async def _report_failure(self, exc: BaseException) -> None:
        if self.health is None:
            return
        try:
            await self.health.failure(self.health_source, exception_summary(exc))
        except Exception as report_exc:
            logger.error(
                "子源 %s 健康失败状态上报失败：%s",
                self.health_source,
                exception_summary(report_exc),
            )

    async def aclose(self) -> None:
        await self._client.aclose()

    close = aclose

    async def __aenter__(self) -> HyperliquidRestClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()
