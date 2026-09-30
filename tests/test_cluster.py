from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from tidewatch.models import Event
from tidewatch.pipeline.cluster import Clusterer
from tidewatch.store.repo import insert_event, save_grading
from tidewatch.store.tables import event_clusters

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
NFP = "美国9月非农就业人口增加8.2万人，预期增加12万人，前值增加14.2万人"


async def _add(conn, clusterer, title, *, now=NOW, category="news", source="jin10", assets=None, level="B", key=None):
    event = Event(
        source=source,
        category=category,
        event_type="flash" if category == "news" else "spot_listing",
        occurred_at=now,
        title=title,
        dedupe_key=key or f"{source}:{title}:{now.isoformat()}",
        assets=assets or [],
        raw={"exchange": source.removesuffix("_ann")} if category == "exchange" else {},
    )
    await insert_event(conn, event, now)
    event.rule_level = event.final_level = level
    event.ai_status = "skipped"
    await save_grading(conn, event)
    return await clusterer.assign(conn, event, now)


def _clusterer(settings) -> Clusterer:
    return Clusterer(settings.pipeline, settings.rules.exchange)


async def test_similar_news_join_same_cluster(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        first = await _add(conn, clusterer, NFP, level="B")
        second = await _add(conn, clusterer, f"【{NFP}】", now=NOW + timedelta(minutes=3), level="A", source="other")
    assert first.id == second.id
    assert second.member_count == 2 and second.source_count == 2
    assert second.max_level == "A"


async def test_window_expired_creates_new_cluster(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        first = await _add(conn, clusterer, NFP)
        later = await _add(conn, clusterer, NFP, now=NOW + timedelta(minutes=11))
    assert first.id != later.id


async def test_disjoint_assets_not_merged(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        a = await _add(conn, clusterer, "大涨5%创新高", assets=["BTC"])
        b = await _add(conn, clusterer, "大涨5%创新高！", assets=["ETH"])
    assert a.id != b.id


async def test_listing_news_joins_announcement_either_order(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        ann = await _add(conn, clusterer, "Binance Will List XYZ (XYZ)", category="exchange", source="binance_ann", assets=["XYZ"], level="S")
        news = await _add(conn, clusterer, "币安将上线XYZ", level="B")
        news2 = await _add(conn, clusterer, "欧易将上线ABC", now=NOW + timedelta(minutes=1))
        ann2 = await _add(conn, clusterer, "OKX to list ABC (ABC) for spot trading", now=NOW + timedelta(minutes=2), category="exchange", source="okx_ann", assets=["ABC"], level="A")
    assert news.id == ann.id and news.source_count == 2 and news.max_level == "S"
    assert ann2.id == news2.id


async def test_different_announcements_not_merged(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        a = await _add(conn, clusterer, "Binance Will List XYZ (XYZ)", category="exchange", source="binance_ann", assets=["XYZ"])
        b = await _add(conn, clusterer, "Binance Will List XYZW (XYZW)", category="exchange", source="binance_ann", assets=["XYZW"])
        count = len((await conn.execute(select(event_clusters.c.id))).all())
    assert a.id != b.id and count == 2


async def test_listing_news_without_symbol_not_merged(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        ann = await _add(conn, clusterer, "Binance Will List XYZ (XYZ)", category="exchange", source="binance_ann", assets=["XYZ"])
        news = await _add(conn, clusterer, "币安将上线新币")
    assert news.id != ann.id
