import asyncio
import json
from datetime import timedelta
from typing import AsyncIterator

import httpx
import pytest
import uvicorn

from fakes import free_port, wait_until
from tidewatch.app import _EmbeddedServer
from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.server import create_app


@pytest.fixture
async def seeded(engine, settings, clock):
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    events = [
        ("jin10:1", "美国9月非农就业人口增加8.2万人", 1),
        ("jin10:2", "比特币突破12万美元", 0),
        ("jin10:3", "印度银行提供新服务", 0),
    ]
    await sink.ingest(FetchResult([
        Event(source="jin10", category="news", event_type="flash", occurred_at=clock.now - timedelta(minutes=i + 1),
              title=title, dedupe_key=key, raw={"important": important})
        for i, (key, title, important) in enumerate(events)
    ], {"last_id": "3"}), collector="jin10")
    await sink.stop()
    return engine


@pytest.fixture
async def client(seeded, settings, clock):
    app = create_app(seeded, Broadcaster(), settings, clock=clock)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        yield http


async def test_index_hides_c_by_default(client):
    page = await client.get("/")
    assert page.status_code == 200
    assert "美国9月非农" in page.text and "比特币突破" in page.text and "印度银行" not in page.text
    shown = await client.get("/", params={"show_c": "1"})
    assert "印度银行" in shown.text


async def test_api_timeline_filters(client):
    rows = (await client.get("/api/timeline", params={"asset": "BTC"})).json()
    assert [r["title"] for r in rows] == ["比特币突破12万美元"]
    rows = (await client.get("/api/timeline", params={"level": "S"})).json()
    assert [r["level"] for r in rows] == ["S"] and rows[0]["pushed"] is True


async def test_rows_carry_cluster_id_for_stable_insert_order(client):
    """行上带 data-cid：前端在发生时间相同时据此按簇 id 倒序插入，与服务端排序一致"""
    rows = (await client.get("/api/timeline")).json()
    fragment = await client.get(f"/fragment/cluster/{rows[0]['cluster_id']}")
    assert f'data-cid="{rows[0]["cluster_id"]}"' in fragment.text
    page = await client.get("/")
    assert all(f'data-cid="{r["cluster_id"]}"' in page.text for r in rows)


async def test_fragment_and_status(client):
    rows = (await client.get("/api/timeline")).json()
    fragment = await client.get(f"/fragment/cluster/{rows[0]['cluster_id']}")
    assert fragment.status_code == 200 and fragment.text.strip().startswith("<tr")
    assert (await client.get("/fragment/cluster/999999")).status_code == 404
    status = await client.get("/status")
    assert status.status_code == 200 and "金十快讯" in status.text
    data = (await client.get("/api/status")).json()
    assert data["collectors"][0]["name"] == "jin10" and data["push_stats"]["pending"] == 1
    assert (await client.get("/static/app.js")).status_code == 200


@pytest.fixture
async def live_server(seeded, settings, clock):
    """真实 uvicorn（空闲端口，嵌入当前事件循环）上的网站；返回 (广播器, 基础地址)"""
    broadcaster = Broadcaster()
    port = free_port()
    config = uvicorn.Config(create_app(seeded, broadcaster, settings, clock=clock), host="127.0.0.1", port=port,
                            log_config=None, access_log=False, timeout_graceful_shutdown=2)
    server = _EmbeddedServer(config)
    task = asyncio.create_task(server.serve())
    try:
        await wait_until(lambda: server.started or task.done())
        assert server.started
        yield broadcaster, f"http://127.0.0.1:{port}"
    finally:
        broadcaster.close()  # 先结束 SSE 订阅，uvicorn 才能很快退出
        server.should_exit = True
        await asyncio.wait_for(task, timeout=5)


async def _read_sse_message(lines: AsyncIterator[str]) -> dict[str, str]:
    """从行迭代器中读取一条完整的 SSE 消息（以空行结束），返回 {字段: 值}；忽略注释行（心跳）"""
    message: dict[str, str] = {}
    async for line in lines:
        if line.startswith(":"):
            continue
        if not line:
            if message:
                return message
            continue
        field, _, value = line.partition(":")
        message[field] = value.lstrip(" ")
    raise AssertionError("SSE 流在读到完整消息前就结束了")


async def test_stream_delivers_published_events(live_server):
    broadcaster, base = live_server
    async with httpx.AsyncClient(base_url=base, timeout=5) as http:
        async with http.stream("GET", "/stream") as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            lines = response.aiter_lines()  # 响应体只能迭代一次，多条消息共用同一个迭代器
            await wait_until(lambda: broadcaster.subscriber_count == 1)  # 订阅完成后发布的消息才收得到
            row = {"cluster_id": 42, "title": "中文标题", "level": "S", "assets": ["BTC"]}
            broadcaster.publish("cluster.updated", row)
            message = await asyncio.wait_for(_read_sse_message(lines), timeout=3)
            assert message["event"] == "cluster.updated"
            assert json.loads(message["data"]) == row
            broadcaster.publish("health.changed", {"name": "jin10", "status": "abnormal"})
            message = await asyncio.wait_for(_read_sse_message(lines), timeout=3)
            assert message["event"] == "health.changed" and json.loads(message["data"])["status"] == "abnormal"
