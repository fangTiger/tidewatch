import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import insert, update

from tidewatch.pipeline.decision import freshness_from_settings
from tidewatch.store.tables import collector_state, event_clusters, events, push_log
from tidewatch.web.queries import cluster_rows, collector_status, push_stats, timeline

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


async def _cluster(conn, level, *, committed=None, category="news") -> int:
    return (await conn.execute(insert(event_clusters).values(
        category=category, first_seen_at=NOW, last_seen_at=NOW, max_level=level, committed_level=committed,
    ).returning(event_clusters.c.id))).scalar_one()


async def _event(conn, cid, level, title, *, minutes_ago=1, collected_delay=0, assets=None, source="jin10",
                 url=None) -> int:
    occurred = NOW - timedelta(minutes=minutes_ago)
    return (await conn.execute(insert(events).values(
        source=source, category="news", event_type="flash", occurred_at=occurred,
        collected_at=occurred + timedelta(minutes=collected_delay), title=title, url=url,
        dedupe_key=f"t:{uuid.uuid4().hex}", final_level=level, assets=assets or [], cluster_id=cid,
    ).returning(events.c.id))).scalar_one()


async def test_timeline_representative_filters_and_backfill(engine, settings):
    fresh = freshness_from_settings(settings)
    async with engine.begin() as conn:
        c1 = await _cluster(conn, "S")
        await _event(conn, c1, "B", "B 级先到", minutes_ago=5)
        await _event(conn, c1, "S", "S 级后到", minutes_ago=3, assets=["BTC"])
        c2 = await _cluster(conn, "C")
        await _event(conn, c2, "C", "C 级快讯", minutes_ago=2)
        c3 = await _cluster(conn, "A")
        await _event(conn, c3, "A", "补抓的旧快讯", minutes_ago=200, collected_delay=190)
        c4 = await _cluster(conn, "A", committed="A")
        b_id = await _event(conn, c4, "B", "已推送簇的代表", minutes_ago=4)
        await _event(conn, c4, "A", "同簇另一条", minutes_ago=6)
        await conn.execute(update(event_clusters).where(event_clusters.c.id == c4).values(rep_event_id=b_id))

    rows = await timeline(engine, freshness=fresh)
    titles = [r["title"] for r in rows]
    assert titles == ["S 级后到", "已推送簇的代表", "补抓的旧快讯"]  # 按代表事件发生时间倒序，默认隐藏 C
    by_title = {r["title"]: r for r in rows}
    assert by_title["补抓的旧快讯"]["backfill"] is True and by_title["S 级后到"]["backfill"] is False
    assert by_title["已推送簇的代表"]["pushed"] is True and by_title["S 级后到"]["level"] == "S"
    assert by_title["S 级后到"]["time_local"] == "09-30 09:57:00" and by_title["S 级后到"]["source_name"] == "金十快讯"

    assert [r["title"] for r in await timeline(engine, freshness=fresh, include_c=True)][0] == "C 级快讯"  # 发生时间最新
    assert [r["title"] for r in await timeline(engine, freshness=fresh, level="C")] == ["C 级快讯"]
    assert [r["title"] for r in await timeline(engine, freshness=fresh, asset="BTC")] == ["S 级后到"]
    assert await timeline(engine, freshness=fresh, category="exchange") == []
    assert [r["cluster_id"] for r in await cluster_rows(engine, [c1, c2], freshness=fresh)] == [c2, c1]  # 按发生时间倒序


async def test_collector_status_and_push_stats(engine):
    async with engine.begin() as conn:
        await conn.execute(insert(collector_state).values(
            name="jin10", status="abnormal", mode="normal", consecutive_failures=5, last_error="HTTP 503",
            last_success_at=NOW - timedelta(minutes=10)))
        await conn.execute(insert(collector_state).values(name="binance_ann", status="normal", mode="degraded"))
        cid = await _cluster(conn, "A")
        eid = await _event(conn, cid, "A", "推送统计")
        for status in ("ok", "ok", "failed", "dry_run"):
            await conn.execute(insert(push_log).values(
                cluster_id=cid, event_id=eid, level="A", kind="event", status=status,
                retry_until=NOW + timedelta(hours=1), created_at=NOW))
        await conn.execute(insert(push_log).values(  # 前一天（上海时间）的记录不计入
            cluster_id=cid, event_id=eid, level="A", kind="event", status="ok",
            retry_until=NOW, created_at=NOW - timedelta(days=1)))
    rows = {r["name"]: r for r in await collector_status(engine)}
    assert rows["jin10"]["status_text"] == "异常" and rows["jin10"]["last_error"] == "HTTP 503"
    assert rows["jin10"]["source_name"] == "金十快讯" and rows["jin10"]["last_success"] == "09:50:00"
    assert rows["binance_ann"]["mode_text"] == "降级"
    stats = await push_stats(engine, NOW)
    assert stats["ok"] == 2 and stats["failed"] == 1 and stats["dry_run"] == 1 and stats["expired"] == 0


async def test_row_assets_is_union_of_cluster_events(engine, settings):
    """行的 assets 是簇内所有事件资产的并集（去重、排序），与按资产筛选「簇内任一事件含该资产」的口径一致"""
    fresh = freshness_from_settings(settings)
    async with engine.begin() as conn:
        cid = await _cluster(conn, "A")
        await _event(conn, cid, "A", "代表事件", minutes_ago=5, assets=["BTC"])  # 等级最高、最早：代表事件
        await _event(conn, cid, "B", "第二条事件", minutes_ago=3, assets=["ETH", "BTC"])  # 带来新资产 ETH
        await _event(conn, cid, "B", "第三条事件", minutes_ago=2, assets=["SOL"])
        empty = await _cluster(conn, "A")
        await _event(conn, empty, "A", "没有资产的事件", minutes_ago=10)
    by_title = {r["title"]: r for r in await timeline(engine, freshness=fresh)}
    assert by_title["代表事件"]["assets"] == ["BTC", "ETH", "SOL"]
    assert by_title["没有资产的事件"]["assets"] == []
    assert (await cluster_rows(engine, [cid], freshness=fresh))[0]["assets"] == ["BTC", "ETH", "SOL"]
    # 服务端按资产筛选：簇内任一事件含该资产即命中，筛出的行 assets 必含该资产（前端 matches() 同口径）
    rows = await timeline(engine, freshness=fresh, asset="SOL")
    assert [r["title"] for r in rows] == ["代表事件"] and "SOL" in rows[0]["assets"]


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://example.com/a?b=1", "https://example.com/a?b=1"),
        ("http://example.com/a", "http://example.com/a"),
        ("HTTPS://Example.com/A", "HTTPS://Example.com/A"),
        ("javascript:alert(1)", None),
        ("JavaScript:alert(1)", None),
        (" javascript:alert(1)", None),
        ("java\nscript:alert(1)", None),
        ("data:text/html,<script>alert(1)</script>", None),
        ("vbscript:msgbox(1)", None),
        ("ftp://example.com/a", None),
        ("//example.com/a", None),
        ("example.com/a", None),
        ("https://", None),
        ("http://[::1/a", None),
        ("", None),
        (None, None),
    ],
)
async def test_row_url_only_http_https(engine, settings, url, expected):
    fresh = freshness_from_settings(settings)
    async with engine.begin() as conn:
        cid = await _cluster(conn, "A")
        await _event(conn, cid, "A", "链接测试", url=url)
    [row] = await timeline(engine, freshness=fresh)
    assert row["url"] == expected


async def test_equal_occurred_at_ordered_by_cluster_id_desc(engine, settings):
    """发生时间相同的簇，顺序稳定：cluster_id 大的在前（timeline 与 cluster_rows 一致）"""
    fresh = freshness_from_settings(settings)
    async with engine.begin() as conn:
        ids = []
        for i in range(4):
            cid = await _cluster(conn, "A")
            await _event(conn, cid, "A", f"同时发生 {i}", minutes_ago=1)
            ids.append(cid)
        newer = await _cluster(conn, "A")
        await _event(conn, newer, "A", "更晚发生", minutes_ago=0)
    expected = [newer, *reversed(ids)]
    assert [r["cluster_id"] for r in await timeline(engine, freshness=fresh)] == expected
    # cluster_rows 传入乱序的 id 也得到同样的顺序
    shuffled = [ids[1], newer, ids[3], ids[0], ids[2]]
    assert [r["cluster_id"] for r in await cluster_rows(engine, shuffled, freshness=fresh)] == expected
