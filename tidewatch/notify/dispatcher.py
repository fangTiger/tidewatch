"""推送调度器（系统设计 6.8、9.2）

- S 级立即单独发送；A 级在滑动窗口内最多单独发送 N 条，超出进入汇总缓冲，定时合并发送
- 发送失败先立即退避重试，仍失败标记 failed，由后台定时重试，超过期限标记 expired
- 启动时恢复 A 级滑动窗口，加载未过期的 pending 记录并处理 failed 记录；被取代（superseded）的记录从队列与缓冲中移除
- 用 _tracked 记录已在队列、汇总缓冲或发送中的记录：入队去重，并定时找回因异常遗漏的 pending 记录
- 入队记录先按数据库中的等级路由，S 与 A 分别由独立发送协程处理，避免 A 的网络等待阻塞 S
- 所有状态回写都经 EventSink.push_result 串行执行
- 容错：启动恢复、定时重试中单条记录或单个步骤出错只记录日志，不影响其他记录与后台任务
- 调用顺序：先 attach()，再 start()，最后启动采集器
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Mapping
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.config import DispatchCfg
from tidewatch.notify.base import EventCard, Notifier, SendResult, WhaleDetails
from tidewatch.pipeline.sink import OPEN_STATUSES, EventSink, IngestOutcome
from tidewatch.store.tables import event_clusters, events, hl_wallets, push_log

logger = logging.getLogger(__name__)

# 每条汇总消息最多包含的事件数（飞书请求体有大小限制）
DIGEST_MAX = 30


@dataclass
class _Pending:
    """从数据库加载出的一条待发送推送：状态与期限用于发送前复核，card 是要展示的内容"""

    push_id: int
    status: str
    level: str
    retry_until: datetime
    card: EventCard


class Dispatcher:
    """推送调度器：消费 EventSink 安排的推送，按等级发送、汇总、重试并回写结果"""

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
        # 新记录先进入路由队列，再按数据库中的等级进入独立发送队列。
        self._queue: asyncio.Queue[int] = asyncio.Queue()
        self._a_queue: asyncio.Queue[int] = asyncio.Queue()
        self._s_queue: asyncio.Queue[int] = asyncio.Queue()
        self._digest: list[int] = []
        self._cancelled: set[int] = set()
        self._tracked: set[int] = set()  # 已在队列、汇总缓冲或发送中的记录
        self._completion: dict[int, asyncio.Future[None]] = {}
        self._a_sent: deque[float] = deque()
        self._tasks: list[asyncio.Task] = []
        self._started = False  # 已启动（含启动过程中），防止重复 start 产生重复的后台任务

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
        """处理被取代的推送：不再发送

        - 在汇总缓冲中：直接移除并解除跟踪，不留标记
        - 仍在发送队列或发送中（已跟踪）：记入 _cancelled，处理到它时丢弃（处理结束后清除标记）
        - 未被跟踪：不记录——数据库中它已是 superseded，重试循环不会再取到，记下来只会让集合无限增长
        """
        for push_id in push_ids:
            if push_id in self._digest:
                self._digest.remove(push_id)
                self._tracked.discard(push_id)
            elif push_id in self._tracked:
                self._cancelled.add(push_id)

    @property
    def digest_size(self) -> int:
        """汇总缓冲中等待合并发送的 A 级推送数量"""
        return len(self._digest)

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        """启动：恢复 A 级窗口与未完成记录，立即处理一次 failed 记录，最后启动后台任务

        恢复与重试各自捕获异常并记录日志，无论成败都会创建后台任务（未处理的记录之后由定时重试找回）。
        已启动时直接返回，不会产生重复任务。
        """
        if self._started:
            return
        self._started = True
        try:
            try:
                await self.load_a_window()
            except Exception:
                logger.exception("启动恢复 A 级滑动窗口失败，本次运行从空窗口开始")
            try:
                await self.load_open()
            except Exception:
                logger.exception("启动恢复未完成推送失败，稍后由定时重试找回")
            self._tasks = [
                asyncio.create_task(self._route_loop(), name="dispatch-route"),
                asyncio.create_task(self._send_loop(), name="dispatch-send"),
                asyncio.create_task(self._s_send_loop(), name="dispatch-send-s"),
            ]
            try:
                await self.retry_failed()
            except Exception:
                logger.exception("启动时重试失败推送出错，稍后由定时重试继续")
            self._tasks.extend(
                [
                    asyncio.create_task(
                        self._periodic(self._cfg.digest_interval_s, self.flush_digest), name="dispatch-digest"
                    ),
                    asyncio.create_task(
                        self._periodic(self._cfg.retry_interval_s, self.retry_failed), name="dispatch-retry"
                    ),
                ]
            )
        except BaseException:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []
            self._started = False  # 启动过程被取消等：允许再次 start
            raise

    def background_tasks(self) -> tuple[asyncio.Task, ...]:
        """返回当前后台任务的只读快照，供应用层守护。"""
        return tuple(self._tasks)

    async def stop(self) -> None:
        """停止：取消并等待所有后台任务退出（任务内的异常被吞掉），之后可以再次 start()"""
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        self._started = False

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

    async def load_a_window(self) -> None:
        """按 sent_at 恢复窗口内已单独送达的 A 级记录，并换算成 monotonic 时间点"""
        wall_now = self._clock()
        mono_now = self._monotonic()
        cutoff = wall_now - timedelta(seconds=self._cfg.a_window_s)
        async with self._engine.connect() as conn:
            sent_times = (
                await conn.execute(
                    select(push_log.c.sent_at)
                    .where(
                        push_log.c.status.in_(("ok", "dry_run")),
                        push_log.c.level == "A",
                        push_log.c.batch_id.is_(None),
                        push_log.c.sent_at > cutoff,
                        push_log.c.sent_at <= wall_now,
                    )
                    .order_by(push_log.c.sent_at)
                )
            ).scalars().all()
        self._a_sent.clear()
        self._a_sent.extend(mono_now - (wall_now - sent_at).total_seconds() for sent_at in sent_times)
        if sent_times:
            logger.info("启动恢复：A 级滑动窗口内已有 %d 条单独推送", len(sent_times))

    async def process_pending(self) -> None:
        """同步处理发送队列中当前所有待发送记录后返回，不启动后台任务

        用于测试，以及需要「处理完再继续」的场景（正常运行时由 _send_loop 持续消费队列）。
        """
        while not self._queue.empty():
            await self._route(self._queue.get_nowait())
        while not self._s_queue.empty():
            await self._handle(self._s_queue.get_nowait())
        while not self._a_queue.empty():
            await self._handle(self._a_queue.get_nowait())

    async def _route_loop(self) -> None:
        """后台任务：按 push_log.level 把新记录送入 S 或 A 发送队列"""
        while True:
            push_id = await self._queue.get()
            try:
                await self._route(push_id)
            except asyncio.CancelledError:
                # 路由途中停止时记录已离开队列；解除跟踪，重启后 load_open 才能重新找回。
                self._tracked.discard(push_id)
                self._cancelled.discard(push_id)
                raise
            except Exception:
                self._tracked.discard(push_id)
                self._cancelled.discard(push_id)
                logger.exception("路由推送失败：%s", push_id)

    async def _route(self, push_id: int) -> None:
        """读取推送记录的等级并路由；记录不存在或已被取代时解除跟踪"""
        if push_id in self._cancelled:
            self._tracked.discard(push_id)
            self._cancelled.discard(push_id)
            return
        async with self._engine.connect() as conn:
            level = (
                await conn.execute(select(push_log.c.level).where(push_log.c.id == push_id))
            ).scalar_one_or_none()
        if level is None:
            self._tracked.discard(push_id)
            return
        queue = self._s_queue if level == "S" else self._a_queue
        queue.put_nowait(push_id)

    async def _send_loop(self) -> None:
        """后台任务：持续从 A 级发送队列取出推送并处理，单条出错只记录日志"""
        while True:
            push_id = await self._a_queue.get()
            try:
                await self._handle(push_id)
            except Exception:
                logger.exception("处理推送失败：%s", push_id)
            finally:
                self._finish_completion(push_id)

    async def _s_send_loop(self) -> None:
        """后台任务：持续从独立的 S 级发送队列取出推送并处理"""
        while True:
            push_id = await self._s_queue.get()
            try:
                await self._handle(push_id)
            except Exception:
                logger.exception("处理 S 级推送失败：%s", push_id)
            finally:
                self._finish_completion(push_id)

    def _finish_completion(self, push_id: int) -> None:
        """唤醒等待本条后台重试完成的调用方；普通入队记录没有等待者"""
        future = self._completion.pop(push_id, None)
        if future is not None and not future.done():
            future.set_result(None)

    async def _periodic(self, interval: float, func: Callable[[], Awaitable[None]]) -> None:
        """后台任务：每隔 interval 秒执行一次 func，func 出错只记录日志，下一轮继续"""
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
            if push_id in self._cancelled:  # _load 期间可能被新事件取代
                self._cancelled.discard(push_id)
                return
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
                self._cancelled.discard(push_id)  # 处理结束后标记不再有用（发送期间被取代的也在此清除）

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
        """单独发送一条推送（含立即退避重试），并把结果回写 push_log

        S 级 @所有人，A 级不 @；发送成功的状态取决于通知器（正式为 ok，演练为 dry_run），失败为 failed。
        """
        result = await self._send_with_retry(
            lambda: self._notifier.send_event(pending.card, mention_all=pending.level == "S"),
            pending.retry_until,
        )
        if result is None:
            await self._sink.push_result(pending.push_id, "expired")
            return
        status = self._notifier.status_on_success if result.ok else "failed"
        await self._sink.push_result(pending.push_id, status, response=result.detail)

    async def _send_with_retry(
        self, send: Callable[[], Awaitable[SendResult]], retry_until: datetime
    ) -> SendResult | None:
        """在期限内立即退避重试；期限已过返回 None，等待不超过剩余期限"""
        delay = 1.0
        for attempt in range(self._cfg.retry_immediate + 1):
            if retry_until <= self._clock():
                return None
            result = await send()
            if result.ok or not result.retryable or attempt == self._cfg.retry_immediate:
                break
            remaining = (retry_until - self._clock()).total_seconds()
            if remaining <= 0:
                continue
            await self._sleep(min(delay, remaining))
            delay *= 2
        if not result.ok:
            logger.warning("推送发送失败：%s", result.detail)
        return result

    async def flush_digest(self) -> None:
        """把汇总缓冲中的 A 级推送合并为一条消息发送（每条最多 DIGEST_MAX 个，其余留到下一轮）"""
        ids = self._digest[:DIGEST_MAX]  # 被取代的推送在 cancel() 时已从缓冲移除
        self._digest = self._digest[DIGEST_MAX:]
        try:
            items = []
            for push_id in ids:
                pending = await self._load(push_id)
                if pending is not None and pending.status in OPEN_STATUSES:
                    items.append(pending)
            items = [p for p in items if p.push_id not in self._cancelled]  # 加载期间可能被新事件取代
            if not items:
                return
            result: SendResult | None = None
            delay = 1.0
            for attempt in range(self._cfg.retry_immediate + 1):
                while True:
                    items = [p for p in items if p.push_id not in self._cancelled]
                    if not items:
                        return
                    now = self._clock()
                    expired = [p for p in items if p.retry_until <= now]
                    if not expired:
                        break
                    for pending in expired:
                        await self._sink.push_result(pending.push_id, "expired")
                    expired_ids = {p.push_id for p in expired}
                    items = [p for p in items if p.push_id not in expired_ids]
                    if not items:
                        return
                    # 状态回写包含 await，循环到真正发送前没有新的过期项为止。
                result = await self._notifier.send_digest([p.card for p in items])
                if result.ok or not result.retryable or attempt == self._cfg.retry_immediate:
                    break
                remaining = min((p.retry_until - self._clock()).total_seconds() for p in items)
                if remaining <= 0:
                    continue
                await self._sleep(min(delay, remaining))
                delay *= 2
            assert result is not None
            if not result.ok:
                logger.warning("推送发送失败：%s", result.detail)
            # 只有汇总真正发出去才记录批次号；失败的记录不带 batch_id，之后重发会归入新的批次
            status = self._notifier.status_on_success if result.ok else "failed"
            batch_id = uuid.uuid4().hex[:12] if result.ok else None
            for pending in items:
                await self._sink.push_result(pending.push_id, status, response=result.detail, batch_id=batch_id)
        finally:
            # 异常中断的记录仍是 pending，由 retry_failed 找回
            self._tracked.difference_update(ids)
            self._cancelled.difference_update(ids)

    async def retry_failed(self) -> None:
        """定时维护：过期记录标为 expired；重发 failed 记录；找回因异常遗漏的 pending 记录

        重发同样经过 _handle，因此 A 级仍受滑动窗口限制（拿不到名额就回到汇总缓冲），不会刷屏。
        """
        now = self._clock()
        orphan_age = timedelta(seconds=2 * self._cfg.digest_interval_s)
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(
                        push_log.c.id,
                        push_log.c.status,
                        push_log.c.level,
                        push_log.c.retry_until,
                        push_log.c.created_at,
                    )
                    .where(push_log.c.status.in_(OPEN_STATUSES))
                    .order_by(push_log.c.id)
                )
            ).all()
        completions: list[asyncio.Future[None]] = []
        for row in rows:
            if row.id in self._cancelled or row.id in self._tracked:
                continue
            try:
                if row.retry_until <= now:
                    await self._sink.push_result(row.id, "expired")
                elif row.status == "failed" or now - row.created_at > orphan_age:
                    self._tracked.add(row.id)
                    if self._tasks:
                        future = asyncio.get_running_loop().create_future()
                        self._completion[row.id] = future
                        queue = self._s_queue if row.level == "S" else self._a_queue
                        queue.put_nowait(row.id)
                        completions.append(future)
                    else:
                        await self._handle(row.id)
            except Exception:
                # 单条记录出错不能中断整轮维护，其余记录照常处理；出错的记录下一轮再试
                logger.exception("重试推送失败：%s", row.id)
        if completions:
            await asyncio.gather(*completions)

    async def _load(self, push_id: int) -> _Pending | None:
        """从数据库加载推送记录及其事件、簇信息并组装成待发送内容；记录不存在时返回 None"""
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
                events.c.category,
                events.c.amount_usd,
                events.c.raw,
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
            wallet = None
            if row is not None and row.category == "hyperliquid" and isinstance(row.raw, Mapping):
                address = row.raw.get("address")
                if isinstance(address, str):
                    wallet = (
                        await conn.execute(
                            select(
                                hl_wallets.c.note,
                                hl_wallets.c.tier,
                                hl_wallets.c.profile,
                            ).where(hl_wallets.c.address == address)
                        )
                    ).mappings().one_or_none()
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
            whale=self._whale_details(row, wallet),
        )
        return _Pending(row.id, row.status, row.level, row.retry_until, card)

    @staticmethod
    def _whale_details(row, wallet: Mapping[str, object] | None) -> WhaleDetails | None:
        """把巨鲸事件 raw 与钱包名单元数据合并为向后兼容的卡片详情。"""
        if row.category != "hyperliquid" or not isinstance(row.raw, Mapping):
            return None
        raw = row.raw
        address = raw.get("address")
        if not isinstance(address, str) or not address:
            return None
        profile = wallet.get("profile") if wallet is not None else None
        profile = profile if isinstance(profile, Mapping) else {}
        note = wallet.get("note") if wallet is not None else None
        label = note if isinstance(note, str) and note else raw.get("note")
        label = label if isinstance(label, str) and label else None
        tags: list[str] = []
        raw_tags = raw.get("tags")
        if isinstance(raw_tags, list):
            tags.extend(str(tag) for tag in raw_tags if str(tag))
        for key in ("leaderboard_display_name", "hypurrscan_alias"):
            value = profile.get(key)
            if isinstance(value, str) and value and value != label and value not in tags:
                tags.append(value)
        tier = wallet.get("tier") if wallet is not None else None
        if not isinstance(tier, str) or not tier:
            tier = raw.get("tier")
        pnl = raw.get("closed_pnl")
        if pnl is None:
            pnl = raw.get("unrealized_pnl")

        def text_value(key: str, fallback: object = None) -> str | None:
            value = raw.get(key, fallback)
            return None if value is None else str(value)

        return WhaleDetails(
            address=address,
            label=label,
            tags=tags,
            coin=text_value("coin", next(iter(row.assets), None)),
            direction=text_value("direction"),
            quantity=text_value("quantity"),
            amount_usd=text_value("notional_usd", row.amount_usd),
            entry_px=text_value("entry_px"),
            liquidation_px=text_value("liquidation_px"),
            liquidation_distance=text_value("liquidation_distance"),
            configured_leverage=text_value("configured_leverage"),
            actual_leverage=text_value("actual_leverage"),
            high_leverage=bool(raw.get("high_leverage")),
            pnl=None if pnl is None else str(pnl),
            tier=tier if isinstance(tier, str) else None,
        )
