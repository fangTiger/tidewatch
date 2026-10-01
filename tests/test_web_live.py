import asyncio
import json
from datetime import timedelta

import pytest

from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.live import LiveUpdates


async def _next(agen):
    return await asyncio.wait_for(agen.__anext__(), timeout=2)


async def test_publish_subscribe_and_overflow():
    broadcaster = Broadcaster(max_queue=2)
    agen = broadcaster.subscribe()
    first = asyncio.create_task(_next(agen))
    while broadcaster.subscriber_count == 0:  # 等订阅者完成注册（3.11 的 wait_for 会再包一层任务）
        await asyncio.sleep(0)
    broadcaster.publish("cluster.updated", {"cluster_id": 1, "title": "中文标题"})
    message = await first
    assert message["event"] == "cluster.updated"
    assert json.loads(message["data"]) == {"cluster_id": 1, "title": "中文标题"}
    for i in range(3):  # 队列容量 2：第 1 条被丢弃
        broadcaster.publish("tick", {"i": i})
    assert json.loads((await _next(agen))["data"]) == {"i": 1}
    await agen.aclose()
    assert broadcaster.subscriber_count == 0


async def test_live_updates_from_sink_and_health(engine, settings, clock):
    broadcaster = Broadcaster()
    live = LiveUpdates(engine, broadcaster, settings)
    sink = create_event_sink(engine, settings, clock=clock)
    sink.on_ingest(live.on_ingest)
    agen = broadcaster.subscribe()
    pending = asyncio.create_task(_next(agen))
    while broadcaster.subscriber_count == 0:
        await asyncio.sleep(0)
    await sink.start()
    try:
        event = Event(source="jin10", category="news", event_type="flash", occurred_at=clock.now - timedelta(minutes=1),
                      title="美国9月非农就业人口增加8.2万人", dedupe_key="jin10:1", raw={"important": 1})
        await sink.ingest(FetchResult([event], None), collector="jin10")
    finally:
        await sink.stop()
    created = await pending
    assert created["event"] == "event.created"
    await live.process_once()  # 处理一批待刷新的簇
    updated = await _next(agen)
    row = json.loads(updated["data"])
    assert updated["event"] == "cluster.updated" and row["level"] == "S" and row["pushed"] is True
    live.on_health({"name": "jin10", "status": "abnormal"})
    health = await _next(agen)
    assert health["event"] == "health.changed" and json.loads(health["data"])["status"] == "abnormal"
    await agen.aclose()


async def test_broadcaster_close_ends_subscribers():
    """close() 后订阅者的迭代正常结束；之后的新订阅也立即结束"""
    broadcaster = Broadcaster()
    agen = broadcaster.subscribe()
    first = asyncio.create_task(_next(agen))
    while broadcaster.subscriber_count == 0:
        await asyncio.sleep(0)
    broadcaster.publish("tick", {"i": 1})
    assert json.loads((await first)["data"]) == {"i": 1}
    waiting = asyncio.create_task(agen.__anext__())  # 订阅者空闲，正在等待下一条消息
    await asyncio.sleep(0)
    broadcaster.close()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(waiting, timeout=2)
    assert broadcaster.subscriber_count == 0
    late = broadcaster.subscribe()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(late.__anext__(), timeout=2)
    assert broadcaster.subscriber_count == 0
    broadcaster.publish("tick", {"i": 2})  # 关闭后发布不报错


async def test_broadcaster_close_with_full_queue():
    """订阅者队列已满时 close() 也能让它结束（挤掉最旧的消息放入结束标记）"""
    broadcaster = Broadcaster(max_queue=2)
    agen = broadcaster.subscribe()
    first = asyncio.create_task(_next(agen))
    while broadcaster.subscriber_count == 0:
        await asyncio.sleep(0)
    broadcaster.publish("tick", {"i": 0})
    await first
    for i in range(1, 4):  # 订阅者不消费，队列被填满
        broadcaster.publish("tick", {"i": i})
    broadcaster.close()

    async def drain() -> list[int]:
        return [json.loads(message["data"])["i"] async for message in agen]

    received = await asyncio.wait_for(drain(), timeout=2)  # 迭代必须结束而不是挂起
    assert received and received[-1] == 3 and len(received) <= 2  # 容量 2：最多剩两条消息，结束标记不占消息
