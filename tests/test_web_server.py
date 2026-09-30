from datetime import timedelta

import httpx
import pytest

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
