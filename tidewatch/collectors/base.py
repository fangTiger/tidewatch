"""采集器契约与基类（系统设计 5.1）"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Awaitable, Callable

from tidewatch.clock import Clock, utcnow
from tidewatch.config import CollectorCfg
from tidewatch.models import FetchResult
from tidewatch.store.repo import load_cursor

logger = logging.getLogger(__name__)

Sleep = Callable[[float], Awaitable[None]]


def describe(exc: BaseException) -> str:
    """异常的简短中文可读描述"""
    return f"{type(exc).__name__}：{exc}"[:300]


class TokenBucket:
    """令牌桶：每秒补充 rate 个令牌，容量 capacity；没有令牌时等待"""

    def __init__(
        self,
        rate_per_s: float,
        capacity: float | None = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._rate = rate_per_s
        self._capacity = capacity or max(1.0, rate_per_s)
        self._tokens = self._capacity
        self._monotonic = monotonic
        self._sleep = sleep
        self._last = monotonic()

    async def acquire(self) -> None:
        while True:
            now = self._monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
            self._last = now
            if self._tokens >= 1:
                self._tokens -= 1
                return
            await self._sleep((1 - self._tokens) / self._rate)


class Backoff:
    """指数退避：1 秒起，每次 ×2，上限 300 秒；成功后 reset"""

    def __init__(self, base: float = 1.0, factor: float = 2.0, cap: float = 300.0) -> None:
        self._base, self._factor, self._cap = base, factor, cap
        self._current = base

    def next(self) -> float:
        value = self._current
        self._current = min(self._current * self._factor, self._cap)
        return value

    def reset(self) -> None:
        self._current = self._base


class Collector(ABC):
    """所有采集器的基类；子类需设置 name 与 group"""

    name: str = ""
    group: str = "news"

    def __init__(
        self,
        cfg: CollectorCfg,
        *,
        sink: Any,
        health: Any,
        client: Any,
        engine: Any,
        clock: Clock = utcnow,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.cfg = cfg
        self.sink = sink
        self.health = health
        self.client = client
        self.engine = engine
        self.clock = clock
        self._sleep = sleep
        self.limiter = TokenBucket(cfg.rate_per_s, sleep=sleep)
        self.backoff = Backoff()

    async def load_cursor(self) -> dict[str, Any] | None:
        async with self.engine.connect() as conn:
            return await load_cursor(conn, self.name)

    async def _load_cursor_with_retry(self) -> dict[str, Any] | None:
        """读取游标；失败（如数据库暂时不可用）时退避重试，不让采集器退出"""
        while True:
            try:
                return await self.load_cursor()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 读取游标失败：%s", self.name, describe(exc))
                await self._sleep(self.backoff.next())

    async def _report_success(self) -> None:
        """上报健康状态；上报本身失败（如数据库断开）只记日志，不影响采集循环"""
        try:
            await self.health.success(self.name)
        except Exception:
            logger.exception("采集器 %s 健康状态上报失败", self.name)

    async def _report_failure(self, exc: BaseException) -> None:
        try:
            await self.health.failure(self.name, describe(exc))
        except Exception:
            logger.exception("采集器 %s 健康状态上报失败", self.name)

    @abstractmethod
    async def run(self) -> None:
        """运行直到被取消"""


class PollCollector(Collector):
    """轮询型采集器：子类实现 fetch(cursor)"""

    @abstractmethod
    async def fetch(self, cursor: dict[str, Any] | None) -> FetchResult: ...

    async def run(self) -> None:
        cursor = await self._load_cursor_with_retry()
        while True:
            try:
                result = await self.fetch(cursor)
                await self._report_success()
                await self.sink.ingest(result, collector=self.name)
                if result.cursor is not None:  # 提交成功后才采用新游标
                    cursor = result.cursor
                self.backoff.reset()
                delay = self.cfg.interval_s
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 本轮失败：%s", self.name, describe(exc))
                await self._report_failure(exc)
                delay = self.backoff.next()
            await self._sleep(delay)


class StreamCollector(Collector):
    """长连接型采集器：子类实现 stream(cursor) 与 gap_fill(cursor)

    每次（重新）连接前先 gap_fill 补缺，连接期间另有后台任务每 gap_fill_interval_s 补缺一次；
    长连接结束（无论正常关闭还是出错）都按失败处理并退避后重连。
    """

    def __init__(self, cfg: CollectorCfg, **kwargs: Any) -> None:
        super().__init__(cfg, **kwargs)
        self.cursor: dict[str, Any] | None = None

    @abstractmethod
    def stream(self, cursor: dict[str, Any] | None) -> AsyncIterator[FetchResult]: ...

    @abstractmethod
    async def gap_fill(self, cursor: dict[str, Any] | None) -> FetchResult: ...

    def merge_cursor(self, old: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any] | None:
        """合并游标；默认新游标覆盖旧游标"""
        return new if new is not None else old

    async def _commit(self, result: FetchResult) -> None:
        """先合并游标再入库，避免较旧的游标覆盖库中较新的游标"""
        merged = self.merge_cursor(self.cursor, result.cursor)
        await self.sink.ingest(FetchResult(result.events, merged, result.records), collector=self.name)
        self.cursor = merged

    async def _gap_fill_loop(self) -> None:
        interval = self.cfg.gap_fill_interval_s or 600
        while True:
            await self._sleep(interval)
            try:
                await self._commit(await self.gap_fill(self.cursor))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 定时补缺失败：%s", self.name, describe(exc))

    async def run(self) -> None:
        self.cursor = await self._load_cursor_with_retry()
        while True:
            filler: asyncio.Task | None = None
            try:
                await self._commit(await self.gap_fill(self.cursor))
                filler = asyncio.create_task(self._gap_fill_loop(), name=f"{self.name}-gap-fill")
                # aclosing：入库失败退出循环时立即关闭长连接
                async with contextlib.aclosing(self.stream(self.cursor)) as messages:
                    async for result in messages:
                        # 只在收到长连接消息时上报成功（补缺成功不代表长连接可用）
                        await self._report_success()
                        if result.events or result.cursor is not None:
                            await self._commit(result)
                        self.backoff.reset()
                raise ConnectionError("长连接已关闭")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 连接中断：%s", self.name, describe(exc))
                await self._report_failure(exc)
                await self._sleep(self.backoff.next())
            finally:
                if filler is not None:
                    filler.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await filler
