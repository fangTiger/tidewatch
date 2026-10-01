import asyncio
import logging

import pytest
from sqlalchemy import select

from fakes import FakeNotifier
from tidewatch.health import HealthMonitor
from tidewatch.notify.base import SendResult
from tidewatch.store.tables import collector_state


def _monitor(engine, settings, clock, notifier) -> HealthMonitor:
    monitor = HealthMonitor(engine, notifier, settings.dispatch, clock=clock)
    monitor.register("jin10", stale_after_s=300)
    return monitor


async def _row(engine):
    async with engine.connect() as conn:
        return (await conn.execute(select(collector_state))).mappings().one()


class _RaisingNotifier(FakeNotifier):
    """指定的第 N 次（从 1 起）send_health 调用抛出异常，其余按预设结果返回"""

    def __init__(self, raise_on: set[int], results: list[SendResult] | None = None) -> None:
        super().__init__(results)
        self.raise_on = raise_on

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        result = await super().send_health(source, healthy, detail)
        if len(self.health) in self.raise_on:
            raise RuntimeError("webhook 异常")
        return result


async def test_five_failures_then_one_alert(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    changes = []
    monitor.on_change(changes.append)
    for _ in range(6):
        await monitor.failure("jin10", "HTTP 503")
    assert monitor.status("jin10") == "abnormal"
    assert [(s, ok) for s, ok, _ in notifier.health] == [("jin10", False)]
    assert changes[-1]["name"] == "jin10" and changes[-1]["status"] == "abnormal"
    async with engine.connect() as conn:
        row = (await conn.execute(select(collector_state))).mappings().one()
    assert row["status"] == "abnormal" and row["consecutive_failures"] == 6 and row["last_error"] == "HTTP 503"


async def test_recovery_notice_and_alert_rate_limit(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await monitor.failure("jin10", "超时")
    await monitor.success("jin10")
    assert monitor.status("jin10") == "normal"
    clock.advance(minutes=10)
    for _ in range(5):  # 1 小时内再次异常：不再发异常告警
        await monitor.failure("jin10", "超时")
    assert [ok for _, ok, _ in notifier.health] == [False, True]
    clock.advance(hours=1)
    await monitor.success("jin10")  # 第二次异常没有告警，恢复时也不发恢复通知
    assert [ok for _, ok, _ in notifier.health] == [False, True]
    for _ in range(5):  # 超过 1 小时后再次异常：重新告警
        await monitor.failure("jin10", "超时")
    assert [ok for _, ok, _ in notifier.health] == [False, True, False]


async def test_load_restores_alert_time_after_restart(engine, settings, clock):
    notifier = FakeNotifier()
    first = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await first.failure("jin10", "超时")
    assert len(notifier.health) == 1
    restarted = _monitor(engine, settings, clock, notifier)  # 模拟进程重启
    await restarted.load()
    assert restarted.status("jin10") == "abnormal"
    await restarted.success("jin10")
    for _ in range(5):
        await restarted.failure("jin10", "超时")
    assert [ok for _, ok, _ in notifier.health] == [False, True]  # 1 小时内不再告警


async def test_stale_detection(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    await monitor.success("jin10")
    clock.advance(seconds=299)
    await monitor.check_stale()
    assert monitor.status("jin10") == "normal"
    clock.advance(seconds=2)
    await monitor.check_stale()
    assert monitor.status("jin10") == "abnormal" and len(notifier.health) == 1


async def test_never_succeeded_uses_register_time(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    clock.advance(seconds=301)
    await monitor.check_stale()
    assert monitor.status("jin10") == "abnormal"


async def test_mode_persisted(engine, settings, clock):
    monitor = _monitor(engine, settings, clock, FakeNotifier())
    await monitor.set_mode("jin10", "degraded")
    async with engine.connect() as conn:
        mode = (await conn.execute(select(collector_state.c.mode))).scalar_one()
    assert mode == "degraded"


# ---------- 失败阈值与成功清零 ----------


@pytest.mark.parametrize(("with_success", "before"), [(True, "normal"), (False, "unknown")])
async def test_abnormal_only_on_fifth_failure(engine, settings, clock, with_success, before):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    if with_success:
        await monitor.success("jin10")
    for _ in range(4):
        await monitor.failure("jin10", "HTTP 503")
    assert monitor.status("jin10") == before and notifier.health == []
    await monitor.failure("jin10", "HTTP 503")
    assert monitor.status("jin10") == "abnormal" and [ok for _, ok, _ in notifier.health] == [False]


async def test_success_resets_failures_persists_and_notifies_listener(engine, settings, clock):
    monitor = _monitor(engine, settings, clock, FakeNotifier())
    changes: list[dict] = []
    monitor.on_change(changes.append)
    await monitor.success("jin10")  # 先进入 normal
    for _ in range(3):
        await monitor.failure("jin10", "超时")
    assert changes[-1]["consecutive_failures"] == 3 and monitor.status("jin10") == "normal"
    clock.advance(seconds=10)
    changes.clear()
    await monitor.success("jin10")  # 状态没变，但失败次数被清零，监听者（状态页）必须知道
    assert monitor.status("jin10") == "normal"
    assert len(changes) == 1 and changes[0]["consecutive_failures"] == 0
    assert changes[0]["last_success_at"] == clock.now.isoformat()
    row = await _row(engine)
    assert row["consecutive_failures"] == 0 and row["last_success_at"] == clock.now


# ---------- 告警发送失败与异常隔离 ----------


async def test_failed_alert_does_not_consume_rate_limit(engine, settings, clock, caplog):
    notifier = FakeNotifier([SendResult(False, retryable=True, detail="HTTP 500")])  # 第一次告警发送失败
    monitor = _monitor(engine, settings, clock, notifier)
    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            await monitor.failure("jin10", "超时")
    assert monitor.status("jin10") == "abnormal" and [ok for _, ok, _ in notifier.health] == [False]
    assert (await _row(engine))["last_alert_at"] is None  # 没有成功告警，不记录告警时间
    assert any("HTTP 500" in r.message for r in caplog.records)
    await monitor.failure("jin10", "超时")  # 仍异常且未成功告警：再次尝试并成功
    assert [ok for _, ok, _ in notifier.health] == [False, False]
    assert (await _row(engine))["last_alert_at"] == clock.now
    await monitor.failure("jin10", "超时")  # 已成功告警：进入限流，不再发送
    assert len(notifier.health) == 2
    await monitor.success("jin10")
    assert [ok for _, ok, _ in notifier.health] == [False, False, True]  # 成功告警过，恢复时发恢复通知


async def test_failed_alert_retried_by_check_stale(engine, settings, clock):
    notifier = FakeNotifier([SendResult(False, detail="HTTP 500")])
    monitor = _monitor(engine, settings, clock, notifier)
    await monitor.success("jin10")
    clock.advance(seconds=301)
    await monitor.check_stale()
    assert monitor.status("jin10") == "abnormal" and len(notifier.health) == 1
    await monitor.check_stale()  # 下一次检查再次尝试
    assert len(notifier.health) == 2 and notifier.health[1][1] is False
    assert "超过 300 秒" in notifier.health[1][2]
    await monitor.check_stale()
    assert len(notifier.health) == 2  # 成功后不再重复


async def test_no_recovery_notice_when_alert_never_succeeded(engine, settings, clock):
    notifier = FakeNotifier([SendResult(False, detail="HTTP 500")])
    monitor = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await monitor.failure("jin10", "超时")
    await monitor.success("jin10")  # 告警从未送达，恢复时也不发「恢复」
    assert monitor.status("jin10") == "normal"
    assert [ok for _, ok, _ in notifier.health] == [False]


async def test_suppressed_alert_sent_after_window_expires(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await monitor.failure("jin10", "超时")
    await monitor.success("jin10")
    assert [ok for _, ok, _ in notifier.health] == [False, True]
    clock.advance(minutes=10)
    for _ in range(6):  # 第二次异常在限流期内：不告警，继续失败也不告警
        await monitor.failure("jin10", "超时")
    assert len(notifier.health) == 2
    clock.advance(hours=1)  # 限流期结束后仍异常：补发一次告警
    await monitor.failure("jin10", "超时")
    assert [ok for _, ok, _ in notifier.health] == [False, True, False]
    await monitor.failure("jin10", "超时")
    assert len(notifier.health) == 3


async def test_alert_exception_does_not_propagate(engine, settings, clock, caplog):
    notifier = _RaisingNotifier({1})  # 第一次告警抛异常，之后正常
    monitor = _monitor(engine, settings, clock, notifier)
    with caplog.at_level(logging.ERROR):
        for _ in range(5):
            await monitor.failure("jin10", "超时")  # 不抛出
    assert monitor.status("jin10") == "abnormal" and (await _row(engine))["last_alert_at"] is None
    assert any(r.levelno == logging.ERROR for r in caplog.records)
    await monitor.failure("jin10", "超时")  # 下一次失败重试告警并成功
    assert [ok for _, ok, _ in notifier.health] == [False, False]
    assert (await _row(engine))["last_alert_at"] == clock.now


async def test_recovery_exception_does_not_propagate(engine, settings, clock):
    notifier = _RaisingNotifier({2})  # 告警成功，恢复通知抛异常
    monitor = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await monitor.failure("jin10", "超时")
    await monitor.success("jin10")  # 不抛出
    assert monitor.status("jin10") == "normal" and [ok for _, ok, _ in notifier.health] == [False, True]
    assert (await _row(engine))["status"] == "normal"


async def test_failed_recovery_notice_is_retried_by_check_stale(engine, settings, clock):
    """恢复通知发送失败后，由下一轮健康检查补发；成功后不再重复。"""
    async def scenario() -> None:
        notifier = FakeNotifier(
            [SendResult(True), SendResult(False, detail="HTTP 500"), SendResult(True)]
        )
        monitor = _monitor(engine, settings, clock, notifier)
        for _ in range(5):
            await monitor.failure("jin10", "超时")

        await monitor.success("jin10")
        assert [ok for _, ok, _ in notifier.health] == [False, True]

        await monitor.check_stale()
        assert [ok for _, ok, _ in notifier.health] == [False, True, True]
        await monitor.check_stale()
        assert [ok for _, ok, _ in notifier.health] == [False, True, True]

    await asyncio.wait_for(scenario(), timeout=5)


async def test_pending_recovery_notice_is_discarded_when_abnormal_again(engine, settings, clock):
    """恢复通知待补发期间再次异常时，丢弃已经过时的恢复通知。"""
    async def scenario() -> None:
        notifier = FakeNotifier([SendResult(True), SendResult(False, detail="HTTP 500")])
        monitor = _monitor(engine, settings, clock, notifier)
        for _ in range(5):
            await monitor.failure("jin10", "第一次异常")
        await monitor.success("jin10")
        assert [ok for _, ok, _ in notifier.health] == [False, True]

        for _ in range(5):
            await monitor.failure("jin10", "再次异常")
        await monitor.success("jin10")
        await monitor.check_stale()

        assert monitor.status("jin10") == "normal"
        assert [ok for _, ok, _ in notifier.health] == [False, True]

    await asyncio.wait_for(scenario(), timeout=5)


async def test_pending_recovery_notice_is_discarded_when_stale_again(engine, settings, clock):
    """恢复通知待补发期间再次超时时，应先转为异常而不是补发过时恢复。"""
    async def scenario() -> None:
        notifier = FakeNotifier([SendResult(True), SendResult(False, detail="HTTP 500")])
        monitor = _monitor(engine, settings, clock, notifier)
        for _ in range(5):
            await monitor.failure("jin10", "第一次异常")
        await monitor.success("jin10")

        clock.advance(seconds=301)
        await monitor.check_stale()
        await monitor.success("jin10")
        await monitor.check_stale()

        assert monitor.status("jin10") == "normal"
        assert [ok for _, ok, _ in notifier.health] == [False, True]

    await asyncio.wait_for(scenario(), timeout=5)


async def test_never_alerted_exception_then_success_does_not_raise(engine, settings, clock):
    notifier = _RaisingNotifier({1, 2, 3})
    monitor = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await monitor.failure("jin10", "超时")
    await monitor.success("jin10")
    assert monitor.status("jin10") == "normal" and len(notifier.health) == 1


# ---------- 超时起点与重启 ----------


async def test_restart_measures_stale_from_registration(engine, settings, clock):
    notifier = FakeNotifier()
    first = _monitor(engine, settings, clock, notifier)
    await first.success("jin10")  # 持久化 last_success_at
    clock.advance(hours=10)  # 长时间停机或电脑休眠后重启
    restarted = _monitor(engine, settings, clock, notifier)
    await restarted.load()
    assert restarted.status("jin10") == "normal"
    await restarted.check_stale()  # 从登记时刻起算，不能因为很早的 last_success 立刻误报
    assert restarted.status("jin10") == "normal" and notifier.health == []
    clock.advance(seconds=299)
    await restarted.check_stale()
    assert restarted.status("jin10") == "normal"
    clock.advance(seconds=2)
    await restarted.check_stale()
    assert restarted.status("jin10") == "abnormal" and len(notifier.health) == 1


# ---------- 并发 ----------


class _GatedNotifier(FakeNotifier):
    """send_health 进入后等待 release 才返回，用来制造「告警发送中」的时间窗口"""

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        result = await super().send_health(source, healthy, detail)
        self.entered.set()
        await self.release.wait()
        return result


async def test_state_transitions_are_serialized(engine, settings, clock):
    """告警发送期间到来的 success() 必须等待，恢复通知只能在告警发完之后发出"""
    notifier = _GatedNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    tasks: list[asyncio.Task] = []
    try:
        # 所有等待都带超时：即使实现出错（告警提前发出、锁重入等），测试也只会失败而不会挂起
        for _ in range(4):
            await asyncio.wait_for(monitor.failure("jin10", "超时"), 5)
        alerting = asyncio.create_task(monitor.failure("jin10", "超时"))
        tasks.append(alerting)
        await asyncio.wait_for(notifier.entered.wait(), 5)
        recovering = asyncio.create_task(monitor.success("jin10"))
        tasks.append(recovering)
        await asyncio.sleep(0.05)
        assert not recovering.done() and [ok for _, ok, _ in notifier.health] == [False]
        notifier.release.set()
        await asyncio.wait_for(asyncio.gather(alerting, recovering), 5)
        assert monitor.status("jin10") == "normal" and [ok for _, ok, _ in notifier.health] == [False, True]
    finally:
        notifier.release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class _RegisteringNotifier(FakeNotifier):
    """发送告警时登记新的采集器，模拟 check_stale 遍历期间 _states 被修改"""

    def __init__(self) -> None:
        super().__init__()
        self.monitor: HealthMonitor | None = None

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        if self.monitor is not None and source == "jin10":
            self.monitor.register("okx_ann", stale_after_s=300)
        return await super().send_health(source, healthy, detail)


async def test_check_stale_tolerates_registration_during_iteration(engine, settings, clock):
    notifier = _RegisteringNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    notifier.monitor = monitor
    clock.advance(seconds=301)
    await monitor.check_stale()  # 遍历期间字典变化不能抛 RuntimeError
    assert monitor.status("jin10") == "abnormal" and monitor.status("okx_ann") == "unknown"


# ---------- 重启恢复：告警未送达的异常要补发 ----------


async def test_load_resends_alert_that_never_succeeded(engine, settings, clock):
    """重启前异常告警没有成功发出（last_alert_at 为空）：重启后在下一次检查时补发"""
    notifier = FakeNotifier([SendResult(False, detail="HTTP 500")])  # 第一次告警发送失败
    first = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await first.failure("jin10", "超时")
    assert (await _row(engine))["status"] == "abnormal" and (await _row(engine))["last_alert_at"] is None
    restarted = _monitor(engine, settings, clock, notifier)  # 模拟进程重启
    await restarted.load()
    assert restarted.status("jin10") == "abnormal"
    await restarted.check_stale()
    assert [ok for _, ok, _ in notifier.health] == [False, False]  # 补发了告警
    assert notifier.health[1][2] == "上次错误：超时"
    assert (await _row(engine))["last_alert_at"] == clock.now
    await restarted.check_stale()
    assert len(notifier.health) == 2  # 补发成功后不再重复
    await restarted.success("jin10")
    assert [ok for _, ok, _ in notifier.health] == [False, False, True]  # 告警送达过，恢复时发恢复通知


async def test_load_does_not_resend_alert_already_delivered(engine, settings, clock):
    notifier = FakeNotifier()
    first = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await first.failure("jin10", "超时")
    assert len(notifier.health) == 1
    restarted = _monitor(engine, settings, clock, notifier)
    await restarted.load()
    await restarted.check_stale()
    assert len(notifier.health) == 1  # 已经告警过：重启后不重复告警


async def test_load_without_last_error_keeps_default_alert_detail(engine, settings, clock):
    """持久化状态没有 last_error 时，重启补发仍使用默认告警说明"""
    notifier = FakeNotifier([SendResult(False, detail="HTTP 500")])
    first = _monitor(engine, settings, clock, notifier)
    clock.advance(seconds=301)
    await first.check_stale()
    assert (await _row(engine))["last_error"] is None
    restarted = _monitor(engine, settings, clock, notifier)
    await restarted.load()
    await restarted.check_stale()
    assert notifier.health[1][2] == "采集器异常"


# ---------- 休眠/唤醒 ----------


def _run_monitor(engine, settings, clock, notifier, steps: list[dict]) -> tuple[HealthMonitor, dict]:
    """构造一个 run() 由注入的 sleep 驱动的监控器：第 n 次 sleep 把时钟拨动 steps[n]，用完后抛 CancelledError 结束循环"""
    state = {"calls": 0}

    async def fake_sleep(seconds: float) -> None:
        if state["calls"] >= len(steps):
            raise asyncio.CancelledError
        clock.advance(**steps[state["calls"]])
        state["calls"] += 1

    monitor = HealthMonitor(engine, notifier, settings.dispatch, clock=clock, sleep=fake_sleep)
    monitor.register("jin10", stale_after_s=300)
    return monitor, state


async def test_run_does_not_alert_after_system_sleep(engine, settings, clock, caplog):
    """两轮检查间隔远大于检查周期（电脑休眠后唤醒）：重新计算超时，而不是误报异常"""
    notifier = FakeNotifier()
    monitor, state = _run_monitor(engine, settings, clock, notifier, [{"seconds": 30}, {"hours": 8}])
    with caplog.at_level(logging.INFO):
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(monitor.run(), timeout=5)
    assert state["calls"] == 2
    assert monitor.status("jin10") == "unknown" and notifier.health == []
    assert any("检测到系统休眠或暂停约 28800 秒，重新计算超时" in r.getMessage() for r in caplog.records)


async def test_run_still_detects_real_outage_after_sleep(engine, settings, clock):
    """休眠唤醒后的重新计时不会让真正的故障漏报：唤醒后正常间隔下继续没有成功请求，超时后照常告警"""
    notifier = FakeNotifier()
    steps = [{"hours": 8}] + [{"seconds": 30}] * 11  # 唤醒后又过了 330 秒（> stale_after 300）
    monitor, state = _run_monitor(engine, settings, clock, notifier, steps)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(monitor.run(), timeout=5)
    assert state["calls"] == 12
    assert monitor.status("jin10") == "abnormal" and [ok for _, ok, _ in notifier.health] == [False]


async def test_run_sleep_reset_skips_abnormal_collectors(engine, settings, clock):
    """休眠重置只作用于非异常的采集器；已经异常的保持异常，未送达的告警照常补发"""
    notifier = FakeNotifier([SendResult(False, detail="HTTP 500")])
    monitor, _ = _run_monitor(engine, settings, clock, notifier, [{"hours": 8}])
    for _ in range(5):
        await monitor.failure("jin10", "超时")  # 进入异常，首次告警发送失败
    assert len(notifier.health) == 1
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(monitor.run(), timeout=5)
    assert monitor.status("jin10") == "abnormal"
    assert [ok for _, ok, _ in notifier.health] == [False, False]


async def test_check_stale_itself_does_not_treat_clock_jump_as_sleep(engine, settings, clock):
    """休眠检测只在 run() 里：直接调用 check_stale() 时大幅拨动时钟仍按超时处理"""
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    clock.advance(hours=8)
    await monitor.check_stale()
    assert monitor.status("jin10") == "abnormal"
