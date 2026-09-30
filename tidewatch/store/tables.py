"""数据库表定义（SQLAlchemy Core）

一期四张表：events（统一事件）、event_clusters（事件簇与推送状态）、
collector_state（采集器游标与健康）、push_log（推送记录）。
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

metadata = MetaData()

event_clusters = Table(
    "event_clusters",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("category", Text, nullable=False),
    Column("cluster_key", Text),
    Column("first_event_id", BigInteger),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("max_level", String(1), nullable=False),
    Column("committed_level", String(1)),
    Column("delivered_level", String(1)),
    Column("rep_event_id", BigInteger),
    Column("member_count", Integer, nullable=False, server_default=text("1")),
    Column("source_count", Integer, nullable=False, server_default=text("1")),
    Column("note_sent", Boolean, nullable=False, server_default=text("false")),
)
Index("ix_event_clusters_category_first_seen", event_clusters.c.category, event_clusters.c.first_seen_at)

events = Table(
    "events",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("source", Text, nullable=False),
    Column("category", Text, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("ref_date", Date),
    Column("collected_at", DateTime(timezone=True), nullable=False),
    Column("title", Text, nullable=False),
    Column("body", Text, nullable=False, server_default=text("''")),
    Column("url", Text),
    Column("assets", ARRAY(Text), nullable=False, server_default=text("'{}'")),
    Column("amount_usd", Numeric),
    Column("rule_level", String(1)),
    Column("rule_hits", ARRAY(Text), nullable=False, server_default=text("'{}'")),
    Column("ai_status", Text),
    Column("ai_level", String(1)),
    Column("ai_direction", Text),
    Column("ai_summary", Text),
    Column("ai_runner", Text),
    Column("ai_at", DateTime(timezone=True)),
    Column("final_level", String(1)),
    Column("dedupe_key", Text, nullable=False, unique=True),
    Column("cluster_id", BigInteger, ForeignKey("event_clusters.id")),
    Column("raw", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
)
Index("ix_events_collected_at", events.c.collected_at)
Index("ix_events_occurred_at", events.c.occurred_at)
Index("ix_events_cluster_id", events.c.cluster_id)

collector_state = Table(
    "collector_state",
    metadata,
    Column("name", Text, primary_key=True),
    Column("cursor", JSONB),
    Column("status", Text, nullable=False, server_default=text("'unknown'")),
    Column("mode", Text, nullable=False, server_default=text("'normal'")),
    Column("last_success_at", DateTime(timezone=True)),
    Column("last_event_at", DateTime(timezone=True)),
    Column("last_error_at", DateTime(timezone=True)),
    Column("last_error", Text),
    Column("consecutive_failures", Integer, nullable=False, server_default=text("0")),
    Column("last_alert_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
)

push_log = Table(
    "push_log",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("cluster_id", BigInteger, ForeignKey("event_clusters.id"), nullable=False),
    Column("event_id", BigInteger, ForeignKey("events.id"), nullable=False),
    Column("level", String(1), nullable=False),
    Column("kind", Text, nullable=False),  # event / upgrade
    Column("batch_id", Text),
    Column("status", Text, nullable=False),  # pending/ok/failed/expired/superseded/dry_run
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("retry_until", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("sent_at", DateTime(timezone=True)),
    Column("response", Text),
)
Index("ix_push_log_status", push_log.c.status)
Index("ix_push_log_cluster_id", push_log.c.cluster_id)
