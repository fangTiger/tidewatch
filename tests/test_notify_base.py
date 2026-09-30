import logging
from collections import deque
from datetime import datetime, timezone

from tidewatch.clock import format_local
from tidewatch.notify.base import EventCard, source_name
from tidewatch.notify.dryrun import DryRunNotifier

WHEN = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


def _card() -> EventCard:
    return EventCard(push_id=1, cluster_id=1, level="S", title="标题", source="jin10", occurred_at=WHEN,
                     url=None, assets=["BTC"], body="", source_count=1, upgraded=False)


def test_format_local_and_source_name():
    assert format_local(WHEN) == "2026-09-30 10:00:00"
    assert format_local(WHEN, "%H:%M") == "10:00"
    assert source_name("jin10") == "金十快讯" and source_name("unknown") == "unknown"


async def test_dry_run_notifier_records_and_succeeds():
    notifier = DryRunNotifier()
    assert notifier.status_on_success == "dry_run"
    assert (await notifier.send_event(_card(), mention_all=True)).ok
    assert (await notifier.send_digest([_card()])).ok
    assert (await notifier.send_health("jin10", False, "连续失败")).ok
    assert [kind for kind, _ in list(notifier.sent)] == ["event", "digest", "health"]


async def test_dry_run_sent_is_bounded_deque():
    notifier = DryRunNotifier()
    assert isinstance(notifier.sent, deque) and notifier.sent.maxlen == 1000
    for _ in range(1005):
        await notifier.send_digest([_card()])
    assert len(notifier.sent) == 1000  # 超出上限时丢弃最旧记录，避免长期运行内存增长


async def test_dry_run_logs_mention_all_and_health_status(caplog):
    notifier = DryRunNotifier()
    with caplog.at_level(logging.INFO, logger="tidewatch.notify.dryrun"):
        await notifier.send_event(_card(), mention_all=True)
        await notifier.send_event(_card(), mention_all=False)
        await notifier.send_health("jin10", False, "连续失败")
        await notifier.send_health("jin10", True, "恢复")
    messages = [record.getMessage() for record in caplog.records]
    assert "mention_all=True" in messages[0] and "mention_all=False" in messages[1]
    assert "异常" in messages[2] and "连续失败" in messages[2]
    assert "正常" in messages[3] and "异常" not in messages[3]
