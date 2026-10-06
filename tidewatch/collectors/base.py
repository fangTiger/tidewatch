"""采集器契约与基类（系统设计 5.1）"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Awaitable, Callable, Iterator

from tidewatch.clock import Clock, utcnow
from tidewatch.config import CollectorCfg
from tidewatch.models import Event, FetchResult
from tidewatch.store.repo import load_cursor

logger = logging.getLogger(__name__)

Sleep = Callable[[float], Awaitable[None]]

# 长连接持续达到该时长才算「稳定」，此时才重置退避（仅靠心跳不算）
STABLE_CONNECTION_S = 60.0


def describe(exc: BaseException) -> str:
    """异常的简短中文可读描述；异常没有消息时（如 TimeoutError）退回 repr，避免只剩类型名与冒号"""
    text = f"{type(exc).__name__}：{exc}" if str(exc) else repr(exc)
    return text[:300]


def sort_events(events: list[Event]) -> list[Event]:
    """按发生时间升序排序，同一时间按去重键排序：保证先发生的事件先入库，结果顺序稳定可复现"""
    return sorted(events, key=lambda event: (event.occurred_at, event.dedupe_key))


@contextlib.contextmanager
def parse_context(what: str) -> Iterator[None]:
    """单条数据解析失败时，在异常消息里带上条目标识（id、url 等），否则只能看到 KeyError: 'time' 而无从定位"""
    try:
        yield
    except Exception as exc:
        raise RuntimeError(f"{what} 解析失败：{describe(exc)}") from exc


class TokenBucket:
    """加权令牌桶：每秒补充 rate 个令牌，容量 capacity；令牌不足时等待"""

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
        self._lock = asyncio.Lock()

    async def acquire(self, weight: float = 1) -> None:
        """原子获取指定权重；不传参数时保持一期的单令牌行为。"""
        if weight <= 0:
            raise ValueError("令牌权重必须大于 0")
        if weight > self._capacity:
            raise ValueError("令牌权重不能超过桶容量")
        async with self._lock:
            while True:
                now = self._monotonic()
                self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= weight:
                    self._tokens -= weight
                    return
                await self._sleep((weight - self._tokens) / self._rate)


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
    """所有采集器的基类；子类需设置非空的 name（游标、健康状态与日志都以它为键）与 group

    clock 提供业务时间（带时区的 datetime）；monotonic 与 sleep 是一对注入的计时源，限流器与退避都用它们，
    测试里同时替换这两个就能完全控制时间的流逝。
    """

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
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if not self.name:
            raise ValueError(f"{type(self).__name__} 必须设置非空的 name：游标、健康状态与日志都以它为键")
        self.cfg = cfg
        self.sink = sink
        self.health = health
        self.client = client
        self.engine = engine
        self.clock = clock
        self._monotonic = monotonic
        self._sleep = sleep
        self.limiter = TokenBucket(cfg.rate_per_s, monotonic=monotonic, sleep=sleep)
        self.backoff = Backoff()

    async def load_cursor(self) -> dict[str, Any] | None:
        async with self.engine.connect() as conn:
            return await load_cursor(conn, self.name)

    async def _load_cursor_with_retry(self) -> dict[str, Any] | None:
        """读取游标；失败（如数据库暂时不可用）时退避重试，不让采集器退出"""
        while True:
            try:
                cursor = await self.load_cursor()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 读取游标失败：%s", self.name, describe(exc))
                await self._sleep(self.backoff.next())
            else:
                self.backoff.reset()  # 读取游标期间累积的退避不能带入之后的采集
                return cursor

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
                await self.sink.ingest(result, collector=self.name)
                if result.cursor is not None:  # 提交成功后才采用新游标
                    cursor = result.cursor
                self.backoff.reset()
                delay = self.cfg.interval_s
                # 入库成功后才上报成功；入库失败的轮次只记失败，连续失败才能累计到告警阈值
                await self._report_success()
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

    健康上报与退避重置使用同一个标准：收到含事件的数据且入库成功，或连接已持续 STABLE_CONNECTION_S 秒
    时收到心跳，才上报成功并重置退避。刚连上就收到的心跳（如订阅确认）不算：否则「确认后立即断开」的震荡
    每一轮都会上报成功，把连续失败清零，健康监控永远不会告警。
    补缺成功不代表长连接可用，补缺本身不上报成功，原因相同。
    """

    def __init__(self, cfg: CollectorCfg, **kwargs: Any) -> None:
        super().__init__(cfg, **kwargs)
        self.cursor: dict[str, Any] | None = None
        # 串行化「合并游标 → 入库 → 赋值」：补缺任务与长连接消息并发提交时，游标不会被较旧的结果回退
        self._commit_lock = asyncio.Lock()

    @abstractmethod
    def stream(self, cursor: dict[str, Any] | None) -> AsyncIterator[FetchResult]: ...

    @abstractmethod
    async def gap_fill(self, cursor: dict[str, Any] | None) -> FetchResult: ...

    def merge_cursor(self, old: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any] | None:
        """合并游标；默认新游标覆盖旧游标"""
        return new if new is not None else old

    async def _commit(self, result: FetchResult) -> None:
        """先合并游标再入库，避免较旧的游标覆盖库中较新的游标；整个过程持锁，并发提交按顺序执行"""
        async with self._commit_lock:
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
            connected_at: float | None = None  # 长连接开始时刻；补缺阶段失败时为 None
            try:
                await self._commit(await self.gap_fill(self.cursor))
                filler = asyncio.create_task(self._gap_fill_loop(), name=f"{self.name}-gap-fill")
                connected_at = self._monotonic()
                # aclosing：入库失败退出循环或被取消时立即关闭长连接，不等垃圾回收
                async with contextlib.aclosing(self.stream(self.cursor)) as messages:
                    async for result in messages:
                        if result.events or result.cursor is not None:
                            await self._commit(result)  # 入库成功后才往下走
                        # 含事件的数据入库成功，或连接已稳定的心跳，才算健康
                        if result.events or self._monotonic() - connected_at >= STABLE_CONNECTION_S:
                            await self._report_success()
                            self.backoff.reset()
                raise ConnectionError("长连接已关闭")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._report_failure(exc)
                lasted = None if connected_at is None else self._monotonic() - connected_at
                if lasted is not None and lasted >= STABLE_CONNECTION_S:
                    self.backoff.reset()  # 稳定连接了足够久才断开：不沿用此前累积的退避
                delay = self.backoff.next()
                if lasted is None:
                    logger.info("采集器 %s 连接前补缺失败：%s；%g 秒后重试", self.name, describe(exc), delay)
                else:
                    logger.info(
                        "采集器 %s 长连接断开：%s；本次持续 %.0f 秒，%g 秒后重连", self.name, describe(exc), lasted, delay
                    )
                await self._sleep(delay)
            finally:
                if filler is not None:
                    filler.cancel()
                    # 用 asyncio.wait 等待清理：外部取消会从这里正常抛出，不会被吞掉
                    await asyncio.wait({filler})
