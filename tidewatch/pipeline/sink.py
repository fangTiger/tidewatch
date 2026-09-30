"""EventSink：唯一的串行写入入口（系统设计 6.3）

所有写库命令（采集结果 ingest、推送结果 push_result）进入同一个队列，由单个工作协程按顺序执行，
保证聚簇与推送决策不会竞争。事务提交后才通知监听者（推送调度器、SSE 广播）。
监听器必须是同步、不阻塞的函数（例如只把数据放进 asyncio.Queue）。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.models import Event, FetchResult, level_ge
from tidewatch.pipeline.cluster import Clusterer
from tidewatch.pipeline.decision import PushDecider, PushPlan, freshness_from_settings
from tidewatch.pipeline.normalize import AssetMatcher
from tidewatch.pipeline.rules.engine import CategoryRules, RuleContext, build_rule_engine
from tidewatch.store.repo import (
    DELIVERED_STATUSES,
    OPEN_STATUSES,
    PUSH_RESULT_STATUSES,
    insert_event,
    record_push_result,
    save_cursor,
    save_grading,
)

logger = logging.getLogger(__name__)

# OPEN_STATUSES / DELIVERED_STATUSES 定义在 store.repo，这里重新导出，调度器从本模块导入
__all__ = [
    "DELIVERED_STATUSES",
    "OPEN_STATUSES",
    "EventSink",
    "IngestListener",
    "IngestOutcome",
    "PushListener",
    "PushOutcome",
    "create_event_sink",
]


@dataclass
class IngestOutcome:
    """一批采集结果提交后的摘要（只在本批有新事件时才通知监听器）

    Attributes:
        collector: 采集器名称。
        event_ids: 本批新插入事件的 id，按处理顺序排列；因重复被忽略的事件不在其中。
        cluster_ids: 与 event_ids 逐项对应的簇 id。同一个簇收到本批多条事件时会重复出现，
            需要唯一值的监听器请自行去重。
        plans: 本批新安排的推送（已写入 push_log，状态为 pending）；
            每个计划的 superseded_ids 是被它取代的旧推送记录。

    所有监听器收到的是同一个对象，只能读取，不得修改。
    """

    collector: str
    event_ids: list[int] = field(default_factory=list)
    cluster_ids: list[int] = field(default_factory=list)
    plans: list[PushPlan] = field(default_factory=list)


IngestListener = Callable[[IngestOutcome], None]


@dataclass
class PushOutcome:
    """一次推送结果回写后的摘要（推送记录不存在时不通知）

    Attributes:
        push_id: 推送记录 id。
        cluster_id: 推送记录所属的簇 id。
        status: 回写后记录的实际状态。记录已是终态（ok / dry_run / expired / superseded）时
            保持原状态，可能与调用 push_result 时传入的状态不同。

    所有监听器收到的是同一个对象，只能读取，不得修改。
    """

    push_id: int
    cluster_id: int
    status: str


PushListener = Callable[[PushOutcome], None]


class EventSink:
    """唯一的串行写入入口

    生命周期：`await start()` 启动工作协程，`await stop()` 处理完队列后停止；
    未启动、正在停止或工作协程已退出时，提交命令会立即抛 RuntimeError，不会挂起。

    命令（ingest / push_result）依次进入队列，由单个工作协程逐条执行，每条命令一个数据库事务：
    - 命令执行出错：事务回滚，异常交给该命令的调用方，工作协程继续处理后面的命令
    - 工作协程被取消：正在执行的命令与队列中尚未执行的命令，调用方都会收到
      RuntimeError("EventSink 已停止")
    - 事务提交之后才通知监听器；监听器在工作协程内同步调用，必须快速返回、不得阻塞；
      监听器抛出的异常会被记录并隔离，不影响其他监听器，也不影响命令的返回
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        rules: CategoryRules,
        clusterer: Clusterer,
        decider: PushDecider,
        matcher: AssetMatcher,
        ai_enabled: bool,
        clock: Clock = utcnow,
    ) -> None:
        self._engine = engine
        self._rules = rules
        self._clusterer = clusterer
        self._decider = decider
        self._matcher = matcher
        self._ai_enabled = ai_enabled
        self._clock = clock
        self._queue: asyncio.Queue[tuple[Any, tuple, asyncio.Future] | None] = asyncio.Queue()
        self._worker: asyncio.Task | None = None
        self._closing = False
        self._ingest_listeners: list[IngestListener] = []
        self._push_listeners: list[PushListener] = []

    # ---------- 生命周期与监听 ----------

    def on_ingest(self, listener: IngestListener) -> None:
        """注册入库监听器

        每批采集结果提交后，只要包含至少一个新事件就调用一次（全是重复事件的批次不通知）。
        参数 IngestOutcome 由所有监听器共享，监听器不得修改它。
        """
        self._ingest_listeners.append(listener)

    def on_push_result(self, listener: PushListener) -> None:
        """注册推送结果监听器

        每次推送结果回写提交后调用一次；推送记录不存在时不通知。
        参数 PushOutcome 由所有监听器共享，监听器不得修改它。
        """
        self._push_listeners.append(listener)

    async def start(self) -> None:
        """启动工作协程；已启动时不做任何事，必须在运行中的事件循环内调用

        工作协程若已退出（例如被取消），须先调用 stop() 复位，才能再次 start()。
        """
        if self._worker is None:
            self._closing = False
            self._worker = asyncio.create_task(self._run(), name="event-sink")

    async def stop(self) -> None:
        """处理完队列中已有的命令后停止；停止过程中新提交的命令立即报错

        可以重复调用。工作协程已经退出（例如被取消）时只做状态复位。
        """
        worker = self._worker
        if worker is None:
            return
        self._closing = True
        try:
            if not worker.done():
                await self._queue.put(None)
                try:
                    # 用 shield 保护工作协程：如果 stop() 自己被取消，取消只会让这里提前抛出
                    # CancelledError，不会连带取消工作协程，否则队列里已有的命令会被丢弃。
                    # 工作协程仍会处理完队列后自行退出，之后再调用 stop() 即可收尾。
                    await asyncio.shield(worker)
                except asyncio.CancelledError:
                    # stop() 被取消：继续向外传播；否则是工作协程自己被取消而结束，视为已停止
                    if not worker.done() or asyncio.current_task().cancelling():
                        raise
        finally:
            # 只有工作协程确实结束后才复位；stop() 被取消而工作协程仍在运行时保持原状
            if worker.done():
                self._worker = None

    # ---------- 对外命令 ----------

    async def ingest(self, result: FetchResult, *, collector: str) -> list[int]:
        """提交一批采集结果；事务提交后返回新插入的事件 id（重复事件不计入）"""
        return await self._submit(self._do_ingest, result, collector)

    async def push_result(
        self, push_id: int, status: str, *, response: str | None = None, batch_id: str | None = None
    ) -> str | None:
        """回写推送结果（ok / failed / dry_run / expired），返回记录的最终状态；记录不存在时返回 None

        status 非法时在入队前直接抛 ValueError。记录已是终态时不会被覆盖，返回值是记录现有的状态
        （语义见 tidewatch.store.repo.record_push_result）。
        """
        if status not in PUSH_RESULT_STATUSES:
            raise ValueError(f"非法的推送结果状态：{status}")
        return await self._submit(self._do_push_result, push_id, status, response, batch_id)

    async def _submit(self, func, *args):
        if self._worker is None or self._worker.done() or self._closing:
            raise RuntimeError("EventSink 尚未启动、正在停止或工作协程已退出")
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._queue.put((func, args, future))
        return await future

    async def _run(self) -> None:
        current: asyncio.Future | None = None  # 正在执行的命令对应的 future
        try:
            while True:
                item = await self._queue.get()
                if item is None:
                    return
                func, args, future = item
                current = future
                try:
                    result = await func(*args)
                except Exception as exc:  # 异常交给调用方处理，工作协程继续运行
                    if future.done():
                        logger.exception("命令执行失败（调用方已取消）")
                    else:
                        future.set_exception(exc)
                else:
                    if not future.done():
                        future.set_result(result)
                current = None
        finally:
            # 退出（含被取消）时，正在执行的命令与仍在队列中的命令都要立即失败，避免调用方永久等待
            if current is not None and not current.done():
                current.set_exception(RuntimeError("EventSink 已停止"))
            while not self._queue.empty():
                item = self._queue.get_nowait()
                if item is not None and not item[2].done():
                    item[2].set_exception(RuntimeError("EventSink 已停止"))

    # ---------- ingest ----------

    async def _do_ingest(self, result: FetchResult, collector: str) -> list[int]:
        now = self._clock()
        for event in result.events:
            self._prepare(event)
        outcome = IngestOutcome(collector=collector)
        async with self._engine.begin() as conn:
            ctx = RuleContext(conn, now)
            for event in result.events:
                if await insert_event(conn, event, now) is None:
                    continue
                outcome.event_ids.append(event.id)
                plan = await self._process(conn, event, ctx, now)
                outcome.cluster_ids.append(event.cluster_id)
                if plan is not None:
                    outcome.plans.append(plan)
            if result.cursor is not None or outcome.event_ids:
                await save_cursor(conn, collector, result.cursor, now, had_events=bool(outcome.event_ids))
        if outcome.event_ids:  # 大多数轮询批次没有新事件，不必通知
            self._emit(self._ingest_listeners, outcome)
        return outcome.event_ids

    def _prepare(self, event: Event) -> None:
        """合并采集器给出的资产与关注资产识别结果（保持顺序、去重）

        交易所公告只识别标题：正文常列出 xxx/BTC、xxx/USDT 等交易对，会误把关注资产算进来。
        """
        text = event.title if event.category == "exchange" else f"{event.title}\n{event.body}"
        found = self._matcher.find(text)
        event.assets = list(dict.fromkeys([*event.assets, *found]))

    async def _process(self, conn: AsyncConnection, event: Event, ctx: RuleContext, now) -> PushPlan | None:
        try:
            async with conn.begin_nested():
                rule = await self._rules.evaluate(event, ctx)
                event.rule_level, event.rule_hits, event.final_level = rule.level, list(rule.hits), rule.level
                event.ai_status = "pending" if self._ai_enabled and level_ge(rule.level, "B") else "skipped"
                await save_grading(conn, event)
                cluster = await self._clusterer.assign(conn, event, now)
                return await self._decider.decide(conn, cluster.id, now)
        except Exception:
            logger.exception("事件处理失败，已降级为 C 级：%s", event.dedupe_key)
            event.rule_level = event.final_level = "C"
            event.rule_hits = ["error"]
            event.ai_status = "skipped"
            event.cluster_id = None
            await save_grading(conn, event)
            await self._clusterer.create_single(conn, event, now)
            return None

    async def _do_push_result(
        self, push_id: int, status: str, response: str | None, batch_id: str | None
    ) -> str | None:
        async with self._engine.begin() as conn:
            recorded = await record_push_result(
                conn, push_id, status, self._clock(), response=response, batch_id=batch_id
            )
        if recorded is None:
            logger.warning("推送记录不存在：%s", push_id)
            return None
        final_status, cluster_id = recorded
        self._emit(self._push_listeners, PushOutcome(push_id, cluster_id, final_status))
        return final_status

    @staticmethod
    def _emit(listeners: list[Callable[[Any], None]], payload: Any) -> None:
        for listener in listeners:
            try:
                listener(payload)
            except Exception:
                logger.exception("监听器处理失败")


def create_event_sink(
    engine: AsyncEngine,
    settings,
    *,
    clock: Clock = utcnow,
    rules: CategoryRules | None = None,
) -> EventSink:
    """按配置装配 EventSink；rules 可在测试中替换"""
    return EventSink(
        engine,
        rules=rules or build_rule_engine(settings),
        clusterer=Clusterer(settings.pipeline, settings.rules.exchange),
        decider=PushDecider(freshness_from_settings(settings), timedelta(seconds=settings.dispatch.retry_max_s)),
        matcher=AssetMatcher(settings.watchlist.assets),
        ai_enabled=settings.ai.enabled,
        clock=clock,
    )
