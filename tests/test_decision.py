from datetime import datetime, timedelta, timezone

from sqlalchemy import insert, select, update

from tidewatch.pipeline.decision import PushDecider, freshness_from_settings
from tidewatch.store.tables import event_clusters, events, push_log

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
AGES = {"jin10": timedelta(minutes=30), "etf": None}


def _decider() -> PushDecider:
    return PushDecider(lambda source: AGES.get(source, timedelta(minutes=30)), retry_max=timedelta(hours=24))


async def _cluster(conn, *, committed=None, delivered=None) -> int:
    return (
        await conn.execute(
            insert(event_clusters)
            .values(category="news", first_seen_at=NOW, last_seen_at=NOW, max_level="C",
                    committed_level=committed, delivered_level=delivered)
            .returning(event_clusters.c.id)
        )
    ).scalar_one()


async def _member(conn, cluster_id, level, *, minutes_ago=1, source="jin10", key=None) -> int:
    occurred = NOW - timedelta(minutes=minutes_ago)
    return (
        await conn.execute(
            insert(events)
            .values(source=source, category="news", event_type="flash", occurred_at=occurred,
                    collected_at=NOW, title=f"{level} 级", dedupe_key=key or f"{cluster_id}:{level}:{minutes_ago}",
                    final_level=level, rule_level=level, cluster_id=cluster_id)
            .returning(events.c.id)
        )
    ).scalar_one()


async def test_first_push(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        eid = await _member(conn, cid, "A", minutes_ago=10)
        plan = await _decider().decide(conn, cid, NOW)
        row = (await conn.execute(select(push_log))).mappings().one()
        cluster = (await conn.execute(select(event_clusters))).mappings().one()
    assert plan.level == "A" and plan.kind == "event" and plan.event_id == eid and plan.superseded_ids == []
    assert row["status"] == "pending" and row["id"] == plan.push_id
    assert row["retry_until"] == NOW - timedelta(minutes=10) + timedelta(minutes=30)
    assert cluster["committed_level"] == "A" and cluster["rep_event_id"] == eid


async def test_b_first_then_s(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "B", minutes_ago=5)
        sid = await _member(conn, cid, "S", minutes_ago=1)
        plan = await _decider().decide(conn, cid, NOW)
    assert plan.level == "S" and plan.event_id == sid and plan.kind == "event"


async def test_upgrade_after_a_delivered(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn, committed="A", delivered="A")
        await _member(conn, cid, "A", minutes_ago=5)
        await _member(conn, cid, "S", minutes_ago=1)
        plan = await _decider().decide(conn, cid, NOW)
    assert plan.level == "S" and plan.kind == "upgrade" and plan.superseded_ids == []


async def test_pending_a_superseded_by_s(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        aid = await _member(conn, cid, "A", minutes_ago=5)
        first = await _decider().decide(conn, cid, NOW)
        await conn.execute(update(events).where(events.c.id == aid).values(final_level="S"))
        second = await _decider().decide(conn, cid, NOW)
        statuses = {r.id: r.status for r in (await conn.execute(select(push_log.c.id, push_log.c.status))).all()}
    assert second.kind == "event" and second.level == "S"
    assert second.superseded_ids == [first.push_id]
    assert statuses == {first.push_id: "superseded", second.push_id: "pending"}


async def test_same_level_not_repeated(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn, committed="A")
        await _member(conn, cid, "A")
        assert await _decider().decide(conn, cid, NOW) is None
        assert (await conn.execute(select(push_log))).first() is None


async def test_stale_event_not_pushed(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "S", minutes_ago=120)
        assert await _decider().decide(conn, cid, NOW) is None


async def test_b_only_not_pushed(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "B")
        assert await _decider().decide(conn, cid, NOW) is None


async def test_unlimited_freshness_uses_retry_max(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "A", minutes_ago=600, source="etf")
        plan = await _decider().decide(conn, cid, NOW)
        retry_until = (await conn.execute(select(push_log.c.retry_until))).scalar_one()
    assert plan is not None and retry_until == NOW + timedelta(hours=24)


async def test_representative_is_earliest_fresh_event_of_top_level(engine):
    """同簇多条新鲜的同级事件，代表事件取发生时间最早的一条"""
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        earliest = await _member(conn, cid, "A", minutes_ago=5)
        await _member(conn, cid, "A", minutes_ago=2)
        plan = await _decider().decide(conn, cid, NOW)
        rep = (await conn.execute(select(event_clusters.c.rep_event_id))).scalar_one()
    assert plan.event_id == earliest and rep == earliest


async def test_supersede_limited_to_own_cluster(engine):
    """升级只取代本簇的未完成推送，不影响其他簇"""
    async with engine.begin() as conn:
        cid1, cid2 = await _cluster(conn), await _cluster(conn)
        a1 = await _member(conn, cid1, "A", minutes_ago=5)
        await _member(conn, cid2, "A", minutes_ago=5)
        first1 = await _decider().decide(conn, cid1, NOW)
        first2 = await _decider().decide(conn, cid2, NOW)
        await conn.execute(update(events).where(events.c.id == a1).values(final_level="S"))
        upgraded = await _decider().decide(conn, cid1, NOW)
        statuses = {r.id: r.status for r in (await conn.execute(select(push_log.c.id, push_log.c.status))).all()}
    assert upgraded.superseded_ids == [first1.push_id]
    assert statuses[first1.push_id] == "superseded"
    assert statuses[first2.push_id] == "pending"


async def test_failed_a_push_is_superseded_by_s(engine):
    """失败待重试的 A 推送也会被 S 取代"""
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        aid = await _member(conn, cid, "A", minutes_ago=5)
        first = await _decider().decide(conn, cid, NOW)
        await conn.execute(update(push_log).where(push_log.c.id == first.push_id).values(status="failed"))
        await conn.execute(update(events).where(events.c.id == aid).values(final_level="S"))
        second = await _decider().decide(conn, cid, NOW)
        statuses = {r.id: r.status for r in (await conn.execute(select(push_log.c.id, push_log.c.status))).all()}
    assert second.superseded_ids == [first.push_id]
    assert statuses == {first.push_id: "superseded", second.push_id: "pending"}


async def test_freshness_boundary_is_inclusive(engine):
    """发生时间恰好等于新鲜度上限之前视为新鲜（≤），再早一秒则过期"""
    decider = _decider()
    assert decider.is_fresh("jin10", NOW - timedelta(minutes=30), NOW)
    assert not decider.is_fresh("jin10", NOW - timedelta(minutes=30, seconds=1), NOW)
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "S", minutes_ago=30)
        plan = await decider.decide(conn, cid, NOW)
    assert plan is not None and plan.level == "S"


def test_freshness_from_settings(settings):
    lookup = freshness_from_settings(settings)
    assert lookup("jin10") == timedelta(seconds=settings.collectors["jin10"].max_push_age_s)
    assert lookup("hl_monitor") == timedelta(minutes=10)
    assert lookup("hl_discovery") == timedelta(minutes=10)
    settings.hyperliquid.events.max_push_age_s = 321
    assert lookup("hl_monitor") == timedelta(seconds=321)
    assert lookup("unknown_source") == timedelta(minutes=30)  # 未配置的来源默认 30 分钟
    settings.collectors["jin10"].max_push_age_s = None
    assert lookup("jin10") is None  # 显式配置为 None 表示不限
