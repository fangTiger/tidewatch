from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

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


async def test_window_counts_from_first_seen_not_last_seen(engine, settings):
    """窗口自簇的 first_seen_at 起算：+8 分钟并入后 last_seen 推到 +8，+12 分钟仍应新建簇"""
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        first = await _add(conn, clusterer, NFP)
        joined = await _add(conn, clusterer, NFP, now=NOW + timedelta(minutes=8))
        late = await _add(conn, clusterer, NFP, now=NOW + timedelta(minutes=12))
        last_seen = (
            await conn.execute(select(event_clusters.c.last_seen_at).where(event_clusters.c.id == first.id))
        ).scalar_one()
    assert joined.id == first.id
    assert last_seen == NOW + timedelta(minutes=8)  # 若按 last_seen 起算，+12 分钟的事件会被并入
    assert late.id != first.id


async def test_source_count_deduplicates_sources(engine, settings):
    """同一来源的多条事件只算一个来源，member_count 仍按事件数计"""
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        await _add(conn, clusterer, NFP, source="jin10")
        await _add(conn, clusterer, f"{NFP}！", now=NOW + timedelta(minutes=1), source="jin10")
        third = await _add(conn, clusterer, f"【{NFP}】", now=NOW + timedelta(minutes=2), source="other")
    assert third.member_count == 3 and third.source_count == 2


@pytest.mark.parametrize(
    "title",
    [
        "XYZ 大涨",  # 只有币种
        "币安 XYZ 大涨",  # 有交易所名，缺上币/下架关键词
        "XYZ 将上线",  # 有上币关键词，缺交易所名
    ],
)
async def test_cross_category_needs_exchange_name_and_listing_keyword(engine, settings, title):
    """快讯并入公告簇必须同时含交易所名与上币/下架关键词"""
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        ann = await _add(
            conn, clusterer, "Binance Will List XYZ (XYZ)", category="exchange", source="binance_ann", assets=["XYZ"]
        )
        news = await _add(conn, clusterer, title, now=NOW + timedelta(minutes=1))
    assert news.id != ann.id


async def test_tie_on_first_seen_prefers_newest_cluster(engine, settings):
    """first_seen_at 相同时按簇 id 倒序，结果确定：并入较新的簇"""
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        older = await _add(
            conn, clusterer, "Binance Will List XYZ (XYZ)", category="exchange", source="binance_ann", assets=["XYZ"]
        )
        newer = await _add(
            conn, clusterer, "OKX to list XYZ (XYZ) for spot trading", category="exchange", source="okx_ann", assets=["XYZ"]
        )
        news = await _add(conn, clusterer, "币安将上线XYZ", now=NOW + timedelta(minutes=1))
    assert older.id != newer.id and newer.id > older.id
    assert news.id == newer.id


async def test_unsaved_event_rejected(engine, settings):
    """事件尚未入库（id 为 None）时，新建簇与并入簇都应报错，而不是静默写入"""
    clusterer = _clusterer(settings)

    def unsaved(title: str) -> Event:
        event = Event(source="jin10", category="news", event_type="flash", occurred_at=NOW, title=title,
                      dedupe_key=f"jin10:{title}", assets=[])
        event.final_level = "B"
        return event

    async with engine.begin() as conn:
        with pytest.raises(ValueError, match="事件尚未入库"):
            await clusterer.create_single(conn, unsaved("新建簇"), NOW)
        # 抛错发生在写库之前：pytest.raises 吞掉异常后事务会提交，此处确认没有留下半成品簇
        assert (await conn.execute(select(func.count()).select_from(event_clusters))).scalar_one() == 0
    async with engine.begin() as conn:
        await _add(conn, clusterer, NFP)
        with pytest.raises(ValueError, match="事件尚未入库"):
            await clusterer.assign(conn, unsaved(NFP), NOW)
