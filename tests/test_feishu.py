import json
from datetime import datetime, timezone

import httpx
import respx

from tidewatch.notify.base import EventCard
from tidewatch.notify.feishu import FeishuNotifier, sign

HOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/test"
WHEN = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


def _card(level="S", *, upgraded=False, url="https://flash.jin10.com/detail/1", source_count=1) -> EventCard:
    return EventCard(push_id=1, cluster_id=1, level=level, title="美国9月非农就业人口增加8.2万人", source="jin10",
                     occurred_at=WHEN, url=url, assets=["BTC"], body="正文内容", source_count=source_count,
                     upgraded=upgraded)


def _notifier(secret="test-secret") -> FeishuNotifier:
    return FeishuNotifier(HOOK, secret, client=httpx.AsyncClient(trust_env=False), clock=lambda: 1790700000)


def test_sign_known_vector():
    assert sign("1790700000", "test-secret") == "CX4RIQmjYTknzzq/eNcdTHe+fEBC6qxm8tpM81NnOLM="


@respx.mock
async def test_s_card_signed_with_mention():
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0, "msg": "success", "data": {}}))
    result = await _notifier().send_event(_card("S", source_count=3), mention_all=True)
    body = json.loads(route.calls.last.request.content)
    content = body["card"]["elements"][0]["text"]["content"]
    assert result.ok and result.detail == "code=0"
    assert body["msg_type"] == "interactive"
    assert body["timestamp"] == "1790700000" and body["sign"] == "CX4RIQmjYTknzzq/eNcdTHe+fEBC6qxm8tpM81NnOLM="
    assert body["card"]["header"]["template"] == "red"
    assert body["card"]["header"]["title"]["content"].startswith("【S】美国9月非农")
    assert "<at id=all></at>" in content and "2026-09-30 10:00:00" in content
    assert "金十快讯（另有 2 个来源）" in content and "BTC" in content and "正文内容" in content
    assert body["card"]["elements"][1]["actions"][0]["url"] == "https://flash.jin10.com/detail/1"


@respx.mock
async def test_upgraded_a_card_without_secret():
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"StatusCode": 0}))
    result = await _notifier(secret=None).send_event(_card("A", upgraded=True, url=None), mention_all=False)
    body = json.loads(route.calls.last.request.content)
    assert result.ok and "sign" not in body and "timestamp" not in body
    assert body["card"]["header"]["template"] == "orange"
    assert body["card"]["header"]["title"]["content"].startswith("【升级】【A】")
    assert "<at id=all></at>" not in body["card"]["elements"][0]["text"]["content"]
    assert len(body["card"]["elements"]) == 1  # 没有原文链接时不加按钮


@respx.mock
async def test_error_classification():
    notifier = _notifier()
    respx.post(HOOK).mock(side_effect=[
        httpx.Response(200, json={"code": 11232, "msg": "frequency limited"}),
        httpx.Response(200, json={"code": 19021, "msg": "sign match fail"}),
        httpx.Response(502, text="bad gateway"),
        httpx.ConnectError("连接失败"),
    ])
    limited = await notifier.send_event(_card(), mention_all=False)
    bad_sign = await notifier.send_event(_card(), mention_all=False)
    server = await notifier.send_event(_card(), mention_all=False)
    network = await notifier.send_event(_card(), mention_all=False)
    assert (limited.ok, limited.retryable) == (False, True) and "11232" in limited.detail
    assert (bad_sign.ok, bad_sign.retryable) == (False, False)
    assert (server.ok, server.retryable) == (False, True)
    assert (network.ok, network.retryable) == (False, True)


@respx.mock
async def test_digest_and_health_cards():
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0}))
    notifier = _notifier()
    await notifier.send_digest([_card("A"), _card("A", url=None)])
    digest = json.loads(route.calls.last.request.content)["card"]
    assert digest["header"]["title"]["content"] == "📋 A 级汇总（2 条）"
    assert digest["elements"][0]["text"]["content"].count("\n") == 1  # 两行
    await notifier.send_health("jin10", False, "连续失败 5 次")
    health = json.loads(route.calls.last.request.content)["card"]
    assert health["header"]["template"] == "red" and "金十快讯" in health["header"]["title"]["content"]
    await notifier.send_health("jin10", True, "恢复正常")
    recovered = json.loads(route.calls.last.request.content)["card"]
    assert recovered["header"]["template"] == "green"
