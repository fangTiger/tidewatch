from datetime import datetime, timezone

import pytest

from tidewatch.models import Event, FetchResult, RuleResult, level_ge, level_rank, max_level


def test_level_helpers():
    assert level_rank("S") > level_rank("A") > level_rank("B") > level_rank("C") > level_rank(None)
    assert level_ge("A", "B") and level_ge("A", "A") and not level_ge("B", "A")
    assert not level_ge(None, "C")
    assert max_level("B", None, "S", "A") == "S"
    assert max_level(None, None) is None


def test_event_requires_timezone():
    with pytest.raises(ValueError):
        Event(
            source="jin10",
            category="news",
            event_type="flash",
            occurred_at=datetime(2026, 9, 30, 10, 0),
            title="无时区",
            dedupe_key="jin10:1",
        )


def test_event_defaults():
    event = Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc),
        title="标题",
        dedupe_key="jin10:1",
    )
    assert event.assets == [] and event.raw == {} and event.body == ""
    assert event.id is None and event.rule_level is None
    result = FetchResult(events=[event], cursor={"last_id": "1"})
    assert result.records == []
    assert RuleResult(level="C").hits == []
