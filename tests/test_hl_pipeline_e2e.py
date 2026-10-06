from __future__ import annotations

import asyncio
from decimal import Decimal

from sqlalchemy import func, insert, select

from fakes import FakeNotifier
from tidewatch.hyperliquid.detect import PositionSnapshot, detect_events, detect_events_with_state
from tidewatch.models import FetchResult
from tidewatch.notify.dispatcher import Dispatcher
from tidewatch.notify.feishu import FeishuNotifier
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.tables import event_clusters, events, hl_wallets, push_log


ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"


def _position(clock, szi: str, notional: str, *, liquidation_px: str | None = None) -> PositionSnapshot:
    return PositionSnapshot(
        address=ADDRESS,
        dex="",
        coin="BTC",
        szi=Decimal(szi),
        notional_usd=Decimal(notional),
        entry_px=Decimal("90000") if Decimal(szi) else None,
        liquidation_px=Decimal(liquidation_px) if liquidation_px is not None else None,
        leverage=Decimal("20") if Decimal(szi) else None,
        unrealized_pnl=Decimal("123456.78") if Decimal(szi) else None,
        account_value=Decimal("1000000"),
        updated_at=clock.now,
    )


def _plain_texts(node) -> list[str]:
    found: list[str] = []
    if isinstance(node, dict):
        if node.get("tag") == "plain_text" and isinstance(node.get("content"), str):
            found.append(node["content"])
        for value in node.values():
            found.extend(_plain_texts(value))
    elif isinstance(node, list):
        for value in node:
            found.extend(_plain_texts(value))
    return found


async def test_snapshot_risk_a_to_s_flows_through_sink_upgrade_and_card(engine, settings, clock):
    async def scenario() -> None:
        sink = create_event_sink(engine, settings, clock=clock)
        await sink.start()
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    insert(hl_wallets).values(
                        address=ADDRESS,
                        note="核心巨鲸",
                        tier="core",
                        source="manual",
                        profile={"leaderboard_display_name": "榜单鲸"},
                    )
                )

            first_position = _position(clock, "120", "12000000", liquidation_px="95")
            first = detect_events_with_state(
                first_position,
                first_position,
                cfg=settings.hyperliquid,
                mark_px=Decimal("100"),
            )
            assert first.liquidation_risk_state == "A"
            await sink.ingest(FetchResult(first.events, None), collector="hl_monitor")
            async with engine.connect() as conn:
                first_push = (await conn.execute(select(push_log))).mappings().one()
            assert first_push["level"] == "A" and first_push["kind"] == "event"
            await sink.push_result(first_push["id"], "dry_run")

            clock.advance(minutes=1)
            critical = _position(clock, "120", "12000000", liquidation_px="98")
            second = detect_events_with_state(
                first_position,
                critical,
                cfg=settings.hyperliquid,
                mark_px=Decimal("100"),
                liquidation_risk_state=first.liquidation_risk_state,
            )
            await sink.ingest(FetchResult(second.events, None), collector="hl_monitor")

            async with engine.connect() as conn:
                event_rows = (await conn.execute(select(events).order_by(events.c.id))).mappings().all()
                cluster_rows = (await conn.execute(select(event_clusters))).mappings().all()
                pushes = (await conn.execute(select(push_log).order_by(push_log.c.id))).mappings().all()
            assert [row["final_level"] for row in event_rows] == ["A", "S"]
            assert event_rows[0]["cluster_id"] == event_rows[1]["cluster_id"]
            assert len(cluster_rows) == 1 and cluster_rows[0]["member_count"] == 2
            assert [(row["level"], row["kind"], row["status"]) for row in pushes] == [
                ("A", "event", "dry_run"),
                ("S", "upgrade", "pending"),
            ]

            dispatcher = Dispatcher(engine, sink, FakeNotifier(), settings.dispatch, clock=clock)
            pending = await dispatcher._load(pushes[-1]["id"])
            assert pending is not None and pending.card.upgraded is True and pending.card.whale is not None
            card = FeishuNotifier._event_card(pending.card, mention_all=True)
            rendered = "\n".join(_plain_texts(card))
            assert "【升级】【S】" in rendered
            assert "核心巨鲸" in rendered and "0x1234…5678" in rendered
            assert "币种：BTC" in rendered and "层级：核心" in rendered
            assert "距强平：2.00%" in rendered and "⚠ 高杠杆" in rendered
        finally:
            await sink.stop()

    await asyncio.wait_for(scenario(), timeout=10)


async def test_reverse_events_enter_separate_clusters_and_pushes(engine, settings, clock):
    async def scenario() -> None:
        sink = create_event_sink(engine, settings, clock=clock)
        await sink.start()
        try:
            previous = _position(clock, "80", "8000000")
            current = _position(clock, "-60", "6000000")
            detected = detect_events(previous, current, cfg=settings.hyperliquid)
            await sink.ingest(FetchResult(detected, None), collector="hl_monitor")
            async with engine.connect() as conn:
                rows = (await conn.execute(select(events.c.event_type, events.c.cluster_id).order_by(events.c.id))).all()
                push_count = (await conn.execute(select(func.count()).select_from(push_log))).scalar_one()
            assert [row.event_type for row in rows] == ["hl_close", "hl_open"]
            assert rows[0].cluster_id != rows[1].cluster_id
            assert push_count == 2
        finally:
            await sink.stop()

    await asyncio.wait_for(scenario(), timeout=10)


async def test_market_maker_is_stored_as_c_without_push(engine, settings, clock):
    async def scenario() -> None:
        sink = create_event_sink(engine, settings, clock=clock)
        await sink.start()
        try:
            detected = detect_events(
                _position(clock, "0", "0"),
                _position(clock, "600", "60000000"),
                cfg=settings.hyperliquid,
                wallet={"style": "market_maker", "tier": "focus"},
            )
            await sink.ingest(FetchResult(detected, None), collector="hl_monitor")
            async with engine.connect() as conn:
                level = (await conn.execute(select(events.c.final_level))).scalar_one()
                push_count = (await conn.execute(select(func.count()).select_from(push_log))).scalar_one()
            assert level == "C" and push_count == 0
        finally:
            await sink.stop()

    await asyncio.wait_for(scenario(), timeout=10)


async def test_hyperliquid_event_older_than_ten_minutes_is_stored_but_not_pushed(engine, settings, clock):
    async def scenario() -> None:
        sink = create_event_sink(engine, settings, clock=clock)
        await sink.start()
        try:
            current = _position(clock, "600", "60000000")
            stale = PositionSnapshot(**{**current.__dict__, "updated_at": clock.now - settings_age})
            detected = detect_events(_position(clock, "0", "0"), stale, cfg=settings.hyperliquid)
            await sink.ingest(FetchResult(detected, None), collector="hl_monitor")
            async with engine.connect() as conn:
                level = (await conn.execute(select(events.c.final_level))).scalar_one()
                push_count = (await conn.execute(select(func.count()).select_from(push_log))).scalar_one()
            assert level == "S" and push_count == 0
        finally:
            await sink.stop()

    from datetime import timedelta

    settings_age = timedelta(minutes=10, seconds=1)
    await asyncio.wait_for(scenario(), timeout=10)
