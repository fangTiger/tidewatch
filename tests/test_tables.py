from datetime import datetime, timezone

import pytest
from sqlalchemy import DateTime, insert, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError

from tidewatch.store.tables import event_clusters, events, metadata


HL_TABLES = {"hl_wallets", "hl_positions", "hl_fills", "hl_state"}


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


def test_hyperliquid_table_metadata_contract():
    """二期四表的主键、JSONB 与 UTC 时间字段必须符合存储契约。"""
    assert HL_TABLES <= set(metadata.tables)

    wallets = metadata.tables["hl_wallets"]
    positions = metadata.tables["hl_positions"]
    fills = metadata.tables["hl_fills"]
    state = metadata.tables["hl_state"]

    assert [column.name for column in wallets.primary_key.columns] == ["address"]
    assert [column.name for column in positions.primary_key.columns] == ["address", "dex", "coin"]
    assert [column.name for column in fills.primary_key.columns] == ["address", "tid"]
    assert [column.name for column in state.primary_key.columns] == ["name"]

    assert isinstance(wallets.c.profile.type, JSONB)
    assert isinstance(fills.c.liquidation.type, JSONB)
    assert isinstance(state.c.cursor.type, JSONB)

    utc_columns = (
        wallets.c.profile_updated_at,
        wallets.c.last_position_at,
        positions.c.updated_at,
        fills.c.occurred_at,
        state.c.updated_at,
    )
    assert all(isinstance(column.type, DateTime) and column.type.timezone for column in utc_columns)

    assert {index.name for index in wallets.indexes} >= {
        "ix_hl_wallets_tier_excluded",
        "ix_hl_wallets_last_position_at",
    }
    assert {index.name for index in positions.indexes} >= {
        "ix_hl_positions_coin",
        "ix_hl_positions_updated_at",
    }
    assert {index.name for index in fills.indexes} >= {
        "ix_hl_fills_address_occurred_at",
        "ix_hl_fills_coin_occurred_at",
        "ix_hl_fills_occurred_at",
    }
