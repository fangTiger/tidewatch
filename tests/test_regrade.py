import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy import insert, select

from tidewatch.store.tables import events
from tidewatch.tools.regrade import parse_args, regrade_events

NOW = datetime(2026, 10, 1, 2, 0, tzinfo=timezone.utc)


async def _assert_regrade_is_read_only(engine, settings):
    rows = [
        {
            "source": "jin10",
            "category": "news",
            "event_type": "flash",
            "occurred_at": NOW,
            "collected_at": NOW,
            "title": "2027年FOMC票委将在十分钟后讲话",
            "dedupe_key": "jin10:preview",
            "rule_level": "S",
            "rule_hits": ["s.rate_decision"],
            "raw": {"important": 1},
        },
        {
            "source": "jin10",
            "category": "news",
            "event_type": "flash",
            "occurred_at": NOW - timedelta(minutes=1),
            "collected_at": NOW,
            "title": "某加密货币交易所被盗约1亿美元",
            "dedupe_key": "jin10:hack",
            "rule_level": "A",
            "rule_hits": ["s.exchange_hack"],
            "raw": {"important": 0},
        },
        {
            "source": "jin10",
            "category": "news",
            "event_type": "flash",
            "occurred_at": NOW - timedelta(minutes=2),
            "collected_at": NOW,
            "title": "普通市场消息",
            "dedupe_key": "jin10:plain",
            "rule_level": "C",
            "rule_hits": [],
            "raw": {"important": 0},
        },
        {
            "source": "other",
            "category": "news",
            "event_type": "flash",
            "occurred_at": NOW + timedelta(minutes=1),
            "collected_at": NOW,
            "title": "不应被读取",
            "dedupe_key": "other:1",
            "rule_level": "S",
            "rule_hits": [],
            "raw": {"important": 1},
        },
    ]
    async with engine.begin() as conn:
        await conn.execute(insert(events), rows)
    async with engine.connect() as conn:
        before = [dict(row) for row in (await conn.execute(select(events).order_by(events.c.id))).mappings()]

    report = await regrade_events(engine, settings, source="jin10", limit=3)

    async with engine.connect() as conn:
        after = [dict(row) for row in (await conn.execute(select(events).order_by(events.c.id))).mappings()]
    assert report.evaluated == 3
    assert report.old_counts == {"S": 1, "A": 1, "B": 0, "C": 1, "未分级": 0}
    assert report.new_counts == {"S": 0, "A": 1, "B": 1, "C": 1, "未分级": 0}
    assert len(report.changes) == 1
    assert report.changes[0].old_level == "S" and report.changes[0].new_level == "B"
    assert "news.important" in report.changes[0].hits
    assert before == after


async def test_regrade_reports_changes_without_writing_database(engine, settings):
    await asyncio.wait_for(_assert_regrade_is_read_only(engine, settings), timeout=5)


def test_regrade_cli_arguments():
    args = parse_args(["--source", "jin10", "--limit", "20", "--changes"])
    assert (args.source, args.limit, args.changes) == ("jin10", 20, True)
