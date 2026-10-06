"""采集器健康监控（系统设计 5.1、12）

- 连续失败达到阈值、或超过 stale_after 没有成功请求时，采集器进入 abnormal 并发送告警
- 告警只有在通知器确认发送成功后才算「已告警」并记录告警时间（限流据此计算）；
  发送失败或抛异常只记录日志，不影响采集器，下一次失败或下一次超时检查时会再次尝试
- 只有成功发出过告警的异常，恢复时才发送恢复通知
- 恢复通知发送失败时保留内存待发标记，由下一轮超时检查补发；补发前再次异常则丢弃过时通知
- 超时按「上次成功」与「登记时刻」中较晚者起算，避免重启后因很早以前的成功记录立即误报
- 电脑休眠/唤醒：采集器在休眠期间不可能有成功请求，唤醒后若直接检查会全部误报。run() 记录上一轮检查的时刻，
  发现两轮间隔远超检查周期（> 3 倍）就认为系统休眠过，把所有非异常采集器的登记时刻重置为当前时刻再检查；
  这个判断只在 run() 里，check_stale() 本身按时钟如实计算超时
- 重启后：已异常但此前没有成功发出过告警（last_alert_at 为空）的采集器，会在下一次检查时补发告警
- success / failure / check_stale / set_mode 的状态转换由同一把锁串行化
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.config import DispatchCfg
from tidewatch.notify.base import Notifier
from tidewatch.store.tables import collector_state

logger = logging.getLogger(__name__)

FAILURE_THRESHOLD = 5
CHECK_INTERVAL_S = 30
# 两轮检查的间隔超过检查周期的这么多倍，就认为系统休眠或进程被暂停过
SLEEP_GAP_FACTOR = 3

HealthListener = Callable[[dict[str, Any]], None]


@dataclass
class _State:
    stale_after_s: float
    registered_at: datetime
    status: str = "unknown"
    mode: str = "normal"
    last_success: datetime | None = None
    failures: int = 0
    last_alert: datetime | None = None
    last_error: str | None = None
    alerted: bool = False  # 本次异常是否已成功发出告警（决定恢复时是否发恢复通知）
    alert_detail: str = "采集器异常"  # 本次异常的原因，告警发送失败后重试时使用
    recovery_pending: bool = False  # 恢复通知发送失败后，等待下一轮检查补发


class HealthMonitor:
    """采集器健康监控：记录成功与失败、判定异常与恢复、发送告警并持久化状态"""

    def __init__(
        self,
        engine: AsyncEngine,
        notifier: Notifier,
        cfg: DispatchCfg,
        *,
        clock: Clock = utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._engine = engine
        self._notifier = notifier
        self._alert_interval_s = cfg.health_alert_interval_s
        self._clock = clock
        self._sleep = sleep
        self._states: dict[str, _State] = {}
        self._listeners: list[HealthListener] = []
        # 保护状态转换：告警发送期间到来的 success/failure 必须等待，保证告警与恢复通知的先后顺序
        self._lock = asyncio.Lock()

    def on_change(self, listener: HealthListener) -> None:
        self._listeners.append(listener)

    def register(self, name: str, stale_after_s: float) -> None:
        """登记一个需要监控的采集器（或子源）"""
        self._states[name] = _State(stale_after_s=stale_after_s, registered_at=self._clock())

    def status(self, name: str) -> str:
        """返回采集器当前状态：unknown / normal / abnormal"""
        return self._states[name].status

    async def load(self) -> None:
        """启动时从 collector_state 恢复已登记采集器的状态与上次告警时间，避免重启绕过告警限流"""
        names = list(self._states)
        if not names:
            return
        now = self._clock()
        seed = pg_insert(collector_state).values(
            [
                {
                    "name": name,
                    "status": "unknown",
                    "mode": "normal",
                    "consecutive_failures": 0,
                    "updated_at": now,
                }
                for name in names
            ]
        )
        async with self._engine.begin() as conn:
            # 冷启动也要让状态页看到所有已登记子源；已有记录必须原样保留。
            await conn.execute(seed.on_conflict_do_nothing(index_elements=["name"]))
            rows = (
                await conn.execute(select(collector_state).where(collector_state.c.name.in_(names)))
            ).mappings().all()
        for row in rows:
            state = self._states[row["name"]]
            state.status = row["status"] or "unknown"
            state.mode = row["mode"] or "normal"
            state.last_alert = row["last_alert_at"]
            state.last_success = row["last_success_at"]
            state.failures = row["consecutive_failures"] or 0
            state.last_error = row["last_error"]
            if state.last_error:
                state.alert_detail = f"上次错误：{state.last_error}"
            # 只有告警确实发出过（有告警时间）才算已告警；否则重启后由 check_stale 补发
            state.alerted = state.status == "abnormal" and row["last_alert_at"] is not None

    async def set_mode(self, name: str, mode: str) -> None:
        """记录运行模式（normal / degraded），例如币安公告无 API Key 时为 degraded"""
        async with self._lock:
            self._states[name].mode = mode
            await self._persist(name, mode=mode)
            self._emit(name)

    async def success(self, name: str) -> None:
        """一次成功的请求（或长连接心跳）：清零连续失败，从异常恢复时发送恢复通知"""
        async with self._lock:
            state = self._states[name]
            now = self._clock()
            had_failures = state.failures > 0
            state.last_success = now
            state.failures = 0
            await self._persist(name, last_success_at=now, consecutive_failures=0)
            if state.status != "normal":
                await self._change(name, "normal", "恢复正常")  # 内部会通知监听者
            elif had_failures:
                self._emit(name)  # 状态没变，但失败次数被清零，让状态页同步

    async def failure(self, name: str, error: str) -> None:
        """一次失败的请求；连续失败达到阈值时进入异常并告警

        已经异常但告警尚未成功送达（发送失败，或此前在限流期内被抑制）时，
        每次失败都会检查是否可以补发告警。
        """
        async with self._lock:
            state = self._states[name]
            now = self._clock()
            state.failures += 1
            state.last_error = error[:500]
            await self._persist(
                name, last_error_at=now, last_error=state.last_error, consecutive_failures=state.failures
            )
            detail = f"连续失败 {state.failures} 次：{state.last_error}"
            if state.failures >= FAILURE_THRESHOLD and state.status != "abnormal":
                await self._change(name, "abnormal", detail)
            else:
                self._emit(name)  # 让状态页看到失败次数变化
                if state.status == "abnormal" and not state.alerted:
                    state.alert_detail = detail
                    await self._try_alert(name)

    async def check_stale(self) -> None:
        """检查请求是否超时，并补发尚未送达的异常告警或恢复通知"""
        async with self._lock:
            now = self._clock()
            # 遍历快照：告警发送期间可能有新的采集器登记
            for name, state in list(self._states.items()):
                if state.status == "abnormal":
                    if not state.alerted:
                        await self._try_alert(name)
                    continue
                # 从上次成功与登记时刻中较晚者起算：重启后不能因为很早以前的成功记录立即误报
                since = max(state.last_success, state.registered_at) if state.last_success else state.registered_at
                if (now - since).total_seconds() > state.stale_after_s:
                    await self._change(name, "abnormal", f"超过 {int(state.stale_after_s)} 秒没有成功请求")
                elif state.recovery_pending and await self._send(name, True, "恢复正常"):
                    state.recovery_pending = False

    async def run(self) -> None:
        """后台循环：定期检查超时；检测到系统休眠/暂停后先重新计时再检查，避免唤醒后集体误报"""
        last_check = self._clock()
        while True:
            await self._sleep(CHECK_INTERVAL_S)
            try:
                now = self._clock()
                gap = (now - last_check).total_seconds()
                if gap > SLEEP_GAP_FACTOR * CHECK_INTERVAL_S:
                    logger.info("检测到系统休眠或暂停约 %d 秒，重新计算超时", gap)
                    await self._restart_timeouts(now)
                await self.check_stale()
            except Exception:
                logger.exception("健康检查失败")
            finally:
                last_check = self._clock()

    async def _restart_timeouts(self, now: datetime) -> None:
        """把所有非异常采集器的登记时刻重置为 now：休眠期间没有成功请求不算采集器的问题"""
        async with self._lock:
            for state in self._states.values():
                if state.status != "abnormal":
                    state.registered_at = now

    async def _change(self, name: str, status: str, detail: str) -> None:
        """状态转换（调用方须已持有锁）：持久化、通知监听者，并按需发送告警或恢复通知"""
        state = self._states[name]
        previous = state.status
        state.status = status
        await self._persist(name, status=status)
        self._emit(name)
        if status == "abnormal":
            logger.warning("采集器异常：%s，%s", name, detail)
            state.recovery_pending = False
            state.alerted = False
            state.alert_detail = detail
            await self._try_alert(name)
        elif previous == "abnormal":
            logger.info("采集器恢复：%s", name)
            alerted = state.alerted
            state.alerted = False
            if alerted:  # 只有本次异常成功发出过告警才发恢复通知，避免只收到「恢复」
                state.recovery_pending = not await self._send(name, True, detail)

    async def _try_alert(self, name: str) -> None:
        """在不处于限流期时尝试发送异常告警（调用方须已持有锁）

        只有通知器确认发送成功才置 alerted、记录并持久化告警时间；
        发送失败不消耗限流额度，下一次失败或下一次超时检查时会再次尝试。
        """
        state = self._states[name]
        now = self._clock()
        if state.last_alert is not None and (now - state.last_alert).total_seconds() < self._alert_interval_s:
            return
        if await self._send(name, False, state.alert_detail):
            state.last_alert = now
            state.alerted = True
            await self._persist(name, last_alert_at=now)

    async def _send(self, name: str, healthy: bool, detail: str) -> bool:
        """发送健康通知，返回是否成功；发送失败或抛异常都只记录日志，不向外抛出"""
        kind = "恢复通知" if healthy else "异常告警"
        try:
            result = await self._notifier.send_health(name, healthy, detail)
        except Exception:
            logger.exception("采集器%s发送异常：%s", kind, name)
            return False
        if not result.ok:
            logger.warning("采集器%s发送失败：%s，%s", kind, name, result.detail)
            return False
        return True

    def _emit(self, name: str) -> None:
        """把采集器当前状态通知给所有监听者（监听者出错只记录日志）"""
        state = self._states[name]
        payload = {
            "name": name,
            "status": state.status,
            "mode": state.mode,
            "consecutive_failures": state.failures,
            "last_error": state.last_error,
            "last_success_at": state.last_success.isoformat() if state.last_success else None,
        }
        for listener in self._listeners:
            try:
                listener(payload)
            except Exception:
                logger.exception("健康状态监听器失败")

    async def _persist(self, name: str, **values: Any) -> None:
        """把指定字段写入 collector_state（不存在则插入，存在则更新）"""
        values["updated_at"] = self._clock()
        stmt = pg_insert(collector_state).values(name=name, **values)
        stmt = stmt.on_conflict_do_update(index_elements=["name"], set_={k: stmt.excluded[k] for k in values})
        async with self._engine.begin() as conn:
            await conn.execute(stmt)
