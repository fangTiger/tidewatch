import asyncio
import json
import re
from datetime import timedelta
from pathlib import Path
from typing import AsyncIterator

import httpx
import pytest
import uvicorn
from sqlalchemy import update

from fakes import free_port, wait_until
from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.tables import collector_state
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.embedded import EmbeddedServer
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
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as http:
        yield http


async def test_index_hides_c_by_default(client):
    page = await client.get("/")
    assert page.status_code == 200
    assert "美国9月非农" in page.text and "比特币突破" in page.text and "印度银行" not in page.text
    shown = await client.get("/", params={"show_c": "1"})
    assert "印度银行" in shown.text


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes", "on", "ON"])
async def test_show_c_accepts_truthy_strings(client, value):
    response = await asyncio.wait_for(client.get("/api/timeline", params={"show_c": value}), timeout=3)
    assert response.status_code == 200
    assert any(row["level"] == "C" for row in response.json())


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "abc", "", "2"])
async def test_show_c_treats_other_values_as_false(client, value):
    response = await asyncio.wait_for(client.get("/api/timeline", params={"show_c": value}), timeout=3)
    assert response.status_code == 200
    assert all(row["level"] != "C" for row in response.json())


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
    assert set(data["collectors"][0]) >= {
        "status", "mode", "last_success", "last_event", "consecutive_failures", "last_error",
    }
    for field in ("status", "mode", "last-success", "last-event", "failures", "error"):
        assert f'class="{field}"' in status.text
    for push_status in ("ok", "dry_run", "pending", "failed", "expired", "superseded"):
        assert f'data-push-status="{push_status}"' in status.text
    assert (await client.get("/static/app.js")).status_code == 200


async def test_status_marks_abnormal_collector_red(seeded, settings, clock):
    """规范场景：异常采集器行带 st-abnormal，样式表将其显示为红色（旧实现本来就通过）。"""
    async with seeded.begin() as conn:
        await conn.execute(
            update(collector_state)
            .where(collector_state.c.name == "jin10")
            .values(status="abnormal", last_error="接口超时")
        )
    app = create_app(seeded, Broadcaster(), settings, clock=clock)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as http:
        page = await asyncio.wait_for(http.get("/status"), timeout=3)

    assert 'id="s-jin10" class="st-abnormal"' in page.text
    assert "接口超时" in page.text
    css = (Path(__file__).resolve().parents[1] / "tidewatch/web/static/style.css").read_text(encoding="utf-8")
    assert re.search(r"tr\.st-abnormal\s+td\s*\{[^}]*color:\s*var\(--s\)", css)


async def test_stream_response_has_send_timeout(seeded, settings, clock):
    app = create_app(seeded, Broadcaster(), settings, clock=clock)
    route = next(route for route in app.routes if getattr(route, "path", None) == "/stream")
    response = await asyncio.wait_for(route.endpoint(), timeout=2)
    assert response.send_timeout == 10


async def test_timeline_escapes_script_in_title(seeded, settings, clock):
    async def scenario():
        sink = create_event_sink(seeded, settings, clock=clock)
        await sink.start()
        try:
            await sink.ingest(FetchResult([
                Event(source="jin10", category="news", event_type="flash", occurred_at=clock.now,
                      title="<script>alert(1)</script>", dedupe_key="jin10:script", raw={"important": 1})
            ], None), collector="jin10")
        finally:
            await sink.stop()
        app = create_app(seeded, Broadcaster(), settings, clock=clock)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as http:
            return await http.get("/")

    page = await asyncio.wait_for(scenario(), timeout=5)
    assert "<script>alert(1)</script>" not in page.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page.text


async def test_host_allowlist_rejects_forged_get_post_and_sse(seeded, settings, clock):
    app = create_app(seeded, Broadcaster(), settings, clock=clock)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://evil.example:8000") as evil:
        forged_get = await asyncio.wait_for(evil.get("/"), timeout=3)
        assert forged_get.status_code == 400
        forged_post = await asyncio.wait_for(
            evil.post(
                "/whales/actions",
                data={"action": "invalid", "address": "0x" + "1" * 40},
                headers={"Origin": "http://evil.example:8000"},
            ),
            timeout=3,
        )
        assert forged_post.status_code == 400
        forged_sse = await asyncio.wait_for(evil.get("/stream"), timeout=3)
        assert forged_sse.status_code == 400

async def test_host_allowlist_accepts_loopback_and_configured_host(seeded, settings, clock):
    settings.web.host = "dashboard.internal"
    app = create_app(seeded, Broadcaster(), settings, clock=clock)
    transport = httpx.ASGITransport(app=app)
    for host in ("127.0.0.1", "localhost", "[::1]", "dashboard.internal"):
        async with httpx.AsyncClient(transport=transport, base_url=f"http://{host}:8000") as http:
            response = await asyncio.wait_for(http.get("/"), timeout=3)
        assert response.status_code == 200


async def test_host_allowlist_does_not_accept_wildcard_configuration(seeded, settings, clock):
    settings.web.host = "*"
    app = create_app(seeded, Broadcaster(), settings, clock=clock)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://evil.example:8000"
    ) as http:
        response = await asyncio.wait_for(http.get("/"), timeout=3)
    assert response.status_code == 400


@pytest.fixture
async def live_server(seeded, settings, clock):
    """真实 uvicorn（空闲端口，嵌入当前事件循环）上的网站；返回 (广播器, 基础地址)"""
    broadcaster = Broadcaster()
    port = free_port()
    config = uvicorn.Config(create_app(seeded, broadcaster, settings, clock=clock), host="127.0.0.1", port=port,
                            log_config=None, access_log=False, timeout_graceful_shutdown=2)
    server = EmbeddedServer(config)
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
        await wait_until(lambda: broadcaster.subscriber_count == 0, timeout=3)


# ---------- 嵌入式 uvicorn：启动失败的表现 ----------


async def test_embedded_server_port_in_use_raises_runtime_error(seeded, settings, clock, occupied_port):
    """端口被占用时 serve() 抛 RuntimeError（含「端口可能被占用」），而不是让 SystemExit 穿出事件循环"""
    config = uvicorn.Config(create_app(seeded, Broadcaster(), settings, clock=clock), host="127.0.0.1",
                            port=occupied_port, log_config=None, access_log=False)
    server = EmbeddedServer(config)
    with pytest.raises(RuntimeError, match="端口可能被占用") as info:
        await asyncio.wait_for(server.serve(), timeout=10)
    assert f"127.0.0.1:{occupied_port}" in str(info.value)
    assert server.started is False


async def test_embedded_server_does_not_touch_signal_handlers(seeded, settings, clock):
    """嵌入式 uvicorn 不接管信号：serve() 前后进程的 SIGINT / SIGTERM 处理器保持不变"""
    import signal

    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
    config = uvicorn.Config(create_app(seeded, Broadcaster(), settings, clock=clock), host="127.0.0.1",
                            port=free_port(), log_config=None, access_log=False)
    server = EmbeddedServer(config)
    task = asyncio.create_task(server.serve())
    try:
        await wait_until(lambda: server.started or task.done())
        assert server.started
        assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=5)
