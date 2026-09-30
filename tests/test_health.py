from sqlalchemy import select

from fakes import FakeNotifier
from tidewatch.health import HealthMonitor
from tidewatch.store.tables import collector_state


def _monitor(engine, settings, clock, notifier) -> HealthMonitor:
    monitor = HealthMonitor(engine, notifier, settings.dispatch, clock=clock)
    monitor.register("jin10", stale_after_s=300)
    return monitor


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
