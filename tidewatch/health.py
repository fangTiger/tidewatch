"""采集器健康监控（系统设计 5.1、12）"""

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
    alerted: bool = False  # 本次异常是否已发出告警（决定恢复时是否发恢复通知）


class HealthMonitor:
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

    def on_change(self, listener: HealthListener) -> None:
        self._listeners.append(listener)

    def register(self, name: str, stale_after_s: float) -> None:
        """登记一个需要监控的采集器（或子源）"""
        self._states[name] = _State(stale_after_s=stale_after_s, registered_at=self._clock())

    def status(self, name: str) -> str:
        return self._states[name].status

    async def load(self) -> None:
        """启动时从 collector_state 恢复已登记采集器的状态与上次告警时间，避免重启绕过告警限流"""
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(select(collector_state).where(collector_state.c.name.in_(list(self._states))))
            ).mappings().all()
        for row in rows:
            state = self._states[row["name"]]
            state.status = row["status"] or "unknown"
            state.mode = row["mode"] or "normal"
            state.last_alert = row["last_alert_at"]
            state.last_success = row["last_success_at"]
            state.failures = row["consecutive_failures"] or 0
            state.last_error = row["last_error"]
            state.alerted = state.status == "abnormal"

    async def set_mode(self, name: str, mode: str) -> None:
        """记录运行模式（normal / degraded），例如币安公告无 API Key 时为 degraded"""
        self._states[name].mode = mode
        await self._persist(name, mode=mode)
        self._emit(name)

    async def success(self, name: str) -> None:
        """一次成功的请求（或长连接心跳）"""
        state = self._states[name]
        now = self._clock()
        state.last_success = now
        state.failures = 0
        await self._persist(name, last_success_at=now, consecutive_failures=0)
        if state.status != "normal":
            await self._change(name, "normal", "恢复正常")

    async def failure(self, name: str, error: str) -> None:
        """一次失败的请求"""
        state = self._states[name]
        now = self._clock()
        state.failures += 1
        state.last_error = error[:500]
        await self._persist(name, last_error_at=now, last_error=state.last_error, consecutive_failures=state.failures)
        if state.failures >= FAILURE_THRESHOLD and state.status != "abnormal":
            await self._change(name, "abnormal", f"连续失败 {state.failures} 次：{state.last_error}")
        else:
            self._emit(name)  # 让状态页看到失败次数变化

    async def check_stale(self) -> None:
        """检查是否有采集器超过 stale_after 没有成功请求"""
        now = self._clock()
        for name, state in self._states.items():
            since = state.last_success or state.registered_at
            if state.status != "abnormal" and (now - since).total_seconds() > state.stale_after_s:
                await self._change(name, "abnormal", f"超过 {int(state.stale_after_s)} 秒没有成功请求")

    async def run(self) -> None:
        """后台循环：定期检查超时"""
        while True:
            await self._sleep(CHECK_INTERVAL_S)
            try:
                await self.check_stale()
            except Exception:
                logger.exception("健康检查失败")

    async def _change(self, name: str, status: str, detail: str) -> None:
        state = self._states[name]
        previous = state.status
        state.status = status
        now = self._clock()
        await self._persist(name, status=status)
        self._emit(name)
        if status == "abnormal":
            logger.warning("采集器异常：%s，%s", name, detail)
            state.alerted = False
            if state.last_alert is None or (now - state.last_alert).total_seconds() >= self._alert_interval_s:
                state.last_alert = now
                state.alerted = True
                await self._persist(name, last_alert_at=now)
                await self._notifier.send_health(name, False, detail)
        elif previous == "abnormal":
            logger.info("采集器恢复：%s", name)
            if state.alerted:  # 只有本次异常发过告警才发恢复通知，避免只收到「恢复」
                await self._notifier.send_health(name, True, detail)
            state.alerted = False

    def _emit(self, name: str) -> None:
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
        values["updated_at"] = self._clock()
        stmt = pg_insert(collector_state).values(name=name, **values)
        stmt = stmt.on_conflict_do_update(index_elements=["name"], set_={k: stmt.excluded[k] for k in values})
        async with self._engine.begin() as conn:
            await conn.execute(stmt)
