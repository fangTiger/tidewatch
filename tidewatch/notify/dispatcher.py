"""推送调度器（系统设计 6.8、9.2）

- S 级立即单独发送；A 级在滑动窗口内最多单独发送 N 条，超出进入汇总缓冲，定时合并发送
- 发送失败先立即退避重试，仍失败标记 failed，由后台定时重试，超过期限标记 expired
- 启动时加载未过期的 pending 记录并处理 failed 记录；被取代（superseded）的记录从队列与缓冲中移除
- 用 _tracked 记录已在队列、汇总缓冲或发送中的记录：入队去重，并定时找回因异常遗漏的 pending 记录
- 所有状态回写都经 EventSink.push_result 串行执行
- 调用顺序：先 attach()，再 start()，最后启动采集器
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.config import DispatchCfg
from tidewatch.notify.base import EventCard, Notifier, SendResult
from tidewatch.pipeline.sink import OPEN_STATUSES, EventSink, IngestOutcome
from tidewatch.store.tables import event_clusters, events, push_log

logger = logging.getLogger(__name__)

# 每条汇总消息最多包含的事件数（飞书请求体有大小限制）
DIGEST_MAX = 30


@dataclass
class _Pending:
    push_id: int
    status: str
    level: str
    retry_until: datetime
    card: EventCard


class Dispatcher:
    def __init__(
        self,
        engine: AsyncEngine,
        sink: EventSink,
        notifier: Notifier,
        cfg: DispatchCfg,
        *,
        clock: Clock = utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._engine = engine
        self._sink = sink
        self._notifier = notifier
        self._cfg = cfg
        self._clock = clock
        self._sleep = sleep
        self._monotonic = monotonic
        self._queue: asyncio.Queue[int] = asyncio.Queue()
        self._digest: list[int] = []
        self._cancelled: set[int] = set()
        self._tracked: set[int] = set()  # 已在队列、汇总缓冲或发送中的记录
        self._a_sent: deque[float] = deque()
        self._tasks: list[asyncio.Task] = []

    # ---------- 与 EventSink 对接 ----------

    def attach(self) -> None:
        """订阅 EventSink：事务提交后接收新安排的推送与被取代的推送"""
        self._sink.on_ingest(self._on_ingest)

    def _on_ingest(self, outcome: IngestOutcome) -> None:
        for plan in outcome.plans:
            self.cancel(plan.superseded_ids)
            self.enqueue([plan.push_id])

    def enqueue(self, push_ids: list[int]) -> None:
        """放入发送队列；已在跟踪中的记录不重复入队"""
        for push_id in push_ids:
            if push_id in self._tracked:
                continue
            self._tracked.add(push_id)
            self._queue.put_nowait(push_id)

    def cancel(self, push_ids: list[int]) -> None:
        """标记被取代的推送：不再发送，并从汇总缓冲移除"""
        self._cancelled.update(push_ids)
        removed = [i for i in self._digest if i in self._cancelled]
        self._digest = [i for i in self._digest if i not in self._cancelled]
        self._tracked.difference_update(removed)

    @property
    def digest_size(self) -> int:
        return len(self._digest)

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        """启动：加载未完成的推送，立即处理一次 failed 记录，再启动后台任务"""
        await self.load_open()
        await self.retry_failed()
        self._tasks = [
            asyncio.create_task(self._send_loop(), name="dispatch-send"),
            asyncio.create_task(self._periodic(self._cfg.digest_interval_s, self.flush_digest), name="dispatch-digest"),
            asyncio.create_task(self._periodic(self._cfg.retry_interval_s, self.retry_failed), name="dispatch-retry"),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def load_open(self) -> None:
        """启动恢复：把未过期的 pending 记录放回发送队列（failed 由定时重试处理）"""
        now = self._clock()
        async with self._engine.connect() as conn:
            ids = (
                await conn.execute(
                    select(push_log.c.id)
                    .where(push_log.c.status == "pending", push_log.c.retry_until > now)
                    .order_by(push_log.c.id)
                )
            ).scalars().all()
        self.enqueue(list(ids))
        if ids:
            logger.info("启动恢复：继续发送 %d 条未完成推送", len(ids))

    async def process_pending(self) -> None:
        """立即处理队列中所有待发送记录（测试与启动时使用）"""
        while not self._queue.empty():
            await self._handle(self._queue.get_nowait())

    async def _send_loop(self) -> None:
        while True:
            push_id = await self._queue.get()
            try:
                await self._handle(push_id)
            except Exception:
                logger.exception("处理推送失败：%s", push_id)

    async def _periodic(self, interval: float, func: Callable[[], Awaitable[None]]) -> None:
        while True:
            await self._sleep(interval)
            try:
                await func()
            except Exception:
                logger.exception("推送后台任务失败")

    # ---------- 发送 ----------

    async def _handle(self, push_id: int) -> None:
        """处理一条已跟踪的记录；除进入汇总缓冲外，处理结束即解除跟踪"""
        keep = False
        try:
            if push_id in self._cancelled:
                self._cancelled.discard(push_id)
                return
            pending = await self._load(push_id)
            if pending is None or pending.status not in OPEN_STATUSES:
                return
            if pending.retry_until <= self._clock():
                await self._sink.push_result(push_id, "expired")
                return
            if pending.level == "A" and not self._take_a_slot():
                if push_id not in self._digest:
                    self._digest.append(push_id)
                keep = True
                return
            await self._deliver(pending)
        finally:
            if not keep:
                self._tracked.discard(push_id)

    def _take_a_slot(self) -> bool:
        """A 级滑动窗口：窗口内已单独发送的数量未达上限时占用一个名额"""
        now = self._monotonic()
        while self._a_sent and now - self._a_sent[0] >= self._cfg.a_window_s:
            self._a_sent.popleft()
        if len(self._a_sent) < self._cfg.a_window_max:
            self._a_sent.append(now)
            return True
        return False

    async def _deliver(self, pending: _Pending) -> None:
        result = await self._send_with_retry(
            lambda: self._notifier.send_event(pending.card, mention_all=pending.level == "S")
        )
        status = self._notifier.status_on_success if result.ok else "failed"
        await self._sink.push_result(pending.push_id, status, response=result.detail)

    async def _send_with_retry(self, send: Callable[[], Awaitable[SendResult]]) -> SendResult:
        """立即退避重试：可重试的失败最多再试 retry_immediate 次，间隔 1、2、4… 秒"""
        delay = 1.0
        result = await send()
        for _ in range(self._cfg.retry_immediate):
            if result.ok or not result.retryable:
                break
            await self._sleep(delay)
            delay *= 2
            result = await send()
        if not result.ok:
            logger.warning("推送发送失败：%s", result.detail)
        return result

    async def flush_digest(self) -> None:
        """把汇总缓冲中的 A 级推送合并为一条消息发送（每条最多 DIGEST_MAX 个，其余留到下一轮）"""
        ids = [i for i in self._digest if i not in self._cancelled][:DIGEST_MAX]
        self._digest = [i for i in self._digest if i not in ids and i not in self._cancelled]
        try:
            now = self._clock()
            items = []
            for push_id in ids:
                pending = await self._load(push_id)
                if pending is not None and pending.status in OPEN_STATUSES and pending.retry_until > now:
                    items.append(pending)
            if not items:
                return
            batch_id = uuid.uuid4().hex[:12]
            result = await self._send_with_retry(lambda: self._notifier.send_digest([p.card for p in items]))
            status = self._notifier.status_on_success if result.ok else "failed"
            for pending in items:
                await self._sink.push_result(pending.push_id, status, response=result.detail, batch_id=batch_id)
        finally:
            self._tracked.difference_update(ids)  # 异常中断的记录仍是 pending，由 retry_failed 找回

    async def retry_failed(self) -> None:
        """定时维护：过期记录标为 expired；重发 failed 记录；找回因异常遗漏的 pending 记录

        重发同样经过 _handle，因此 A 级仍受滑动窗口限制（拿不到名额就回到汇总缓冲），不会刷屏。
        """
        now = self._clock()
        orphan_age = timedelta(seconds=2 * self._cfg.digest_interval_s)
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(push_log.c.id, push_log.c.status, push_log.c.retry_until, push_log.c.created_at)
                    .where(push_log.c.status.in_(OPEN_STATUSES))
                    .order_by(push_log.c.id)
                )
            ).all()
        for row in rows:
            if row.id in self._cancelled or row.id in self._tracked:
                continue
            if row.retry_until <= now:
                await self._sink.push_result(row.id, "expired")
            elif row.status == "failed" or now - row.created_at > orphan_age:
                self._tracked.add(row.id)
                await self._handle(row.id)

    async def _load(self, push_id: int) -> _Pending | None:
        stmt = (
            select(
                push_log.c.id,
                push_log.c.status,
                push_log.c.level,
                push_log.c.kind,
                push_log.c.retry_until,
                push_log.c.cluster_id,
                events.c.title,
                events.c.body,
                events.c.source,
                events.c.occurred_at,
                events.c.url,
                events.c.assets,
                event_clusters.c.source_count,
            )
            .select_from(
                push_log.join(events, events.c.id == push_log.c.event_id).join(
                    event_clusters, event_clusters.c.id == push_log.c.cluster_id
                )
            )
            .where(push_log.c.id == push_id)
        )
        async with self._engine.connect() as conn:
            row = (await conn.execute(stmt)).one_or_none()
        if row is None:
            return None
        card = EventCard(
            push_id=row.id,
            cluster_id=row.cluster_id,
            level=row.level,
            title=row.title,
            source=row.source,
            occurred_at=row.occurred_at,
            url=row.url,
            assets=list(row.assets),
            body=row.body,
            source_count=row.source_count,
            upgraded=row.kind == "upgrade",
        )
        return _Pending(row.id, row.status, row.level, row.retry_until, card)
