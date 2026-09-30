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
from tidewatch.store.repo import insert_event, save_cursor, save_grading

logger = logging.getLogger(__name__)

# 推送状态：可变（未完成）的状态与视为已送达的状态
OPEN_STATUSES = ("pending", "failed")
DELIVERED_STATUSES = ("ok", "dry_run")


@dataclass
class IngestOutcome:
    """一批采集结果提交后的摘要"""

    collector: str
    event_ids: list[int] = field(default_factory=list)
    cluster_ids: list[int] = field(default_factory=list)
    plans: list[PushPlan] = field(default_factory=list)


IngestListener = Callable[[IngestOutcome], None]


class EventSink:
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

    # ---------- 生命周期与监听 ----------

    def on_ingest(self, listener: IngestListener) -> None:
        self._ingest_listeners.append(listener)

    async def start(self) -> None:
        if self._worker is None:
            self._closing = False
            self._worker = asyncio.create_task(self._run(), name="event-sink")

    async def stop(self) -> None:
        """处理完队列中已有的命令后停止；停止过程中新提交的命令立即报错"""
        if self._worker is None:
            return
        self._closing = True
        await self._queue.put(None)
        await self._worker
        self._worker = None

    # ---------- 对外命令 ----------

    async def ingest(self, result: FetchResult, *, collector: str) -> list[int]:
        """提交一批采集结果；事务提交后返回新插入的事件 id（重复事件不计入）"""
        return await self._submit(self._do_ingest, result, collector)

    async def _submit(self, func, *args):
        if self._worker is None or self._closing:
            raise RuntimeError("EventSink 尚未启动或正在停止")
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._queue.put((func, args, future))
        return await future

    async def _run(self) -> None:
        try:
            while True:
                item = await self._queue.get()
                if item is None:
                    return
                func, args, future = item
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
        finally:
            # 退出（含被取消）时，让仍在队列中的命令立即失败，避免调用方永久等待
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
