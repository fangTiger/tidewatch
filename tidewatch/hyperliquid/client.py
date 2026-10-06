"""Hyperliquid REST ``POST /info`` 客户端。"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal

import httpx

from tidewatch.collectors.base import Backoff, TokenBucket, describe
from tidewatch.collectors.http import make_client

logger = logging.getLogger(__name__)

REST_URL = "https://api.hyperliquid.xyz/info"

Budget = Literal["monitor", "batch"]
Sleep = Callable[[float], Awaitable[None]]

# 系统设计 7.1 明确列出的基础权重；其余轻量 info 请求按官方通用权重 2 处理。
BASE_WEIGHTS: dict[str, int] = {
    "clearinghouseState": 2,
    "spotClearinghouseState": 2,
    "userFills": 20,
    "userFillsByTime": 20,
    "portfolio": 20,
    "userTwapSliceFills": 20,
}
VOLUME_WEIGHT_TYPES = frozenset({"userFills", "userFillsByTime", "userTwapSliceFills"})


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
        logger.info(
            "Hyperliquid REST 使用 %s 模式；切换代理节点会改变限流出口 IP",
            proxy_mode,
        )

    async def info(self, request_type: str, *, budget: Budget = "monitor", **params: Any) -> Any:
        """发送一条 info 请求；JSON 中的数字字符串保持上游原样。"""
        return await self.request({"type": request_type, **params}, budget=budget)

    async def request(self, payload: Mapping[str, Any], *, budget: Budget = "monitor") -> Any:
        request_type = payload.get("type")
        if not isinstance(request_type, str) or not request_type:
            raise ValueError("Hyperliquid info 请求必须包含非空 type")
        bucket = self._bucket(budget)
        base_weight = BASE_WEIGHTS.get(request_type, 2)
        backoff = Backoff()
        limited_since: float | None = None

        while True:
            await bucket.acquire(base_weight)
            try:
                response = await self._client.post(self.url, json=dict(payload))
            except asyncio.CancelledError:
                raise
            except httpx.RequestError as exc:
                await self._report_failure(exc)
                delay = backoff.next()
                logger.warning(
                    "子源 %s 请求 %s（权重 %d）网络失败：%s；%g 秒后重试，代理模式 %s",
                    self.health_source,
                    request_type,
                    base_weight,
                    describe(exc),
                    delay,
                    self.proxy_mode,
                )
                await self._sleep(delay)
                continue

            if response.status_code == 429 or response.status_code >= 500:
                error = httpx.HTTPStatusError(
                    f"Hyperliquid HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                await self._report_failure(error)
                delay = backoff.next()
                if response.status_code == 429:
                    limited_since = limited_since or self._monotonic()
                persistent = (
                    response.status_code == 429
                    and limited_since is not None
                    and self._monotonic() - limited_since >= 300
                )
                state = "持续限流" if persistent else "单次限流" if response.status_code == 429 else "服务端错误"
                logger.warning(
                    "子源 %s 请求 %s（权重 %d）%s HTTP %d；%g 秒后重试，代理模式 %s",
                    self.health_source,
                    request_type,
                    base_weight,
                    state,
                    response.status_code,
                    delay,
                    self.proxy_mode,
                )
                await self._sleep(delay)
                continue

            try:
                response.raise_for_status()
                data = response.json()
                extra_weight = self._response_weight(request_type, data)
                if extra_weight:
                    await bucket.acquire(extra_weight)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._report_failure(exc)
                raise

            backoff.reset()
            await self._report_success()
            return data

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
        except Exception:
            logger.exception("子源 %s 健康成功状态上报失败", self.health_source)

    async def _report_failure(self, exc: BaseException) -> None:
        if self.health is None:
            return
        try:
            await self.health.failure(self.health_source, describe(exc))
        except Exception:
            logger.exception("子源 %s 健康失败状态上报失败", self.health_source)

    async def aclose(self) -> None:
        await self._client.aclose()

    close = aclose

    async def __aenter__(self) -> HyperliquidRestClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()
