from datetime import datetime, timezone

import pytest
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from tidewatch.store.tables import event_clusters, events


def _event_row(key: str) -> dict:
    now = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
    return {
        "source": "jin10",
        "category": "news",
        "event_type": "flash",
        "occurred_at": now,
        "collected_at": now,
        "title": "测试标题",
        "assets": ["BTC", "ETH"],
        "dedupe_key": key,
        "raw": {"important": 1},
    }


async def test_insert_and_read_back(engine):
    async with engine.begin() as conn:
        await conn.execute(insert(events).values(**_event_row("k1")))
        row = (await conn.execute(select(events).where(events.c.dedupe_key == "k1"))).mappings().one()
    assert row["assets"] == ["BTC", "ETH"]
    assert row["raw"] == {"important": 1}
    assert row["body"] == ""
    assert row["occurred_at"].tzinfo is not None


async def test_dedupe_key_unique(engine):
    async with engine.begin() as conn:
        await conn.execute(insert(events).values(**_event_row("dup")))
    with pytest.raises(IntegrityError):
        async with engine.begin() as conn:
            await conn.execute(insert(events).values(**_event_row("dup")))


async def test_cluster_defaults(engine):
    now = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
    async with engine.begin() as conn:
        cid = (
            await conn.execute(
                insert(event_clusters)
                .values(category="news", first_seen_at=now, last_seen_at=now, max_level="B")
                .returning(event_clusters.c.id)
            )
        ).scalar_one()
        row = (await conn.execute(select(event_clusters).where(event_clusters.c.id == cid))).mappings().one()
    assert row["member_count"] == 1 and row["source_count"] == 1
    assert row["committed_level"] is None and row["note_sent"] is False
