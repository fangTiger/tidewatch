import json
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
import respx

from tidewatch.notify import feishu
from tidewatch.notify.base import EventCard
from tidewatch.notify.feishu import FeishuNotifier, sign

HOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/test"
WHEN = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
EVIL_BODY = "重磅 <at id=all></at> [点此领取](https://evil.example)"


def _card(level="S", *, upgraded=False, url="https://flash.jin10.com/detail/1", source_count=1,
          title="美国9月非农就业人口增加8.2万人", body="正文内容", assets=None) -> EventCard:
    return EventCard(push_id=1, cluster_id=1, level=level, title=title, source="jin10",
                     occurred_at=WHEN, url=url, assets=["BTC"] if assets is None else assets, body=body,
                     source_count=source_count, upgraded=upgraded)


@pytest.fixture
async def make_notifier():
    """创建使用注入客户端的通知器，用例结束后统一关闭客户端（通知器不负责关闭注入的客户端）"""
    clients: list[httpx.AsyncClient] = []

    def factory(secret: str | None = "test-secret") -> FeishuNotifier:
        client = httpx.AsyncClient(trust_env=False)
        clients.append(client)
        return FeishuNotifier(HOOK, secret, client=client, clock=lambda: 1790700000)

    yield factory
    for client in clients:
        await client.aclose()


def _walk(node: Any, tag: str) -> list[str]:
    """递归收集卡片中所有 tag 为指定值的文本节点的 content"""
    found: list[str] = []
    if isinstance(node, dict):
        if node.get("tag") == tag and "content" in node:
            found.append(node["content"])
        for value in node.values():
            found.extend(_walk(value, tag))
    elif isinstance(node, list):
        for item in node:
            found.extend(_walk(item, tag))
    return found


def _lark_md(payload: dict[str, Any]) -> list[str]:
    return _walk(payload["card"], "lark_md")


def _plain_text(payload: dict[str, Any]) -> list[str]:
    return _walk(payload["card"], "plain_text")


def _ok_route() -> respx.Route:
    return respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0, "msg": "success", "data": {}}))


def _asset_lines(payload: dict[str, Any]) -> list[str]:
    """事件卡片中的资产行（plain_text div，内容以「资产：」开头）"""
    return [e["text"]["content"] for e in payload["card"]["elements"][1:]
            if e["tag"] == "div" and e["text"]["tag"] == "plain_text" and e["text"]["content"].startswith("资产：")]


def _body_div(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """事件卡片中除首个 lark_md 固定字段块与资产行之外的 div 元素"""
    return [e for e in payload["card"]["elements"][1:]
            if e["tag"] == "div" and not e["text"]["content"].startswith("资产：")]


def _has_button(payload: dict[str, Any]) -> bool:
    return any(e["tag"] == "action" for e in payload["card"]["elements"])


def test_sign_known_vector():
    assert sign("1790700000", "test-secret") == "CX4RIQmjYTknzzq/eNcdTHe+fEBC6qxm8tpM81NnOLM="


# ---------- 基本发送与签名 ----------


@respx.mock
async def test_s_card_signed_with_mention(make_notifier):
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0, "msg": "success", "data": {}}))
    result = await make_notifier().send_event(_card("S", source_count=3), mention_all=True)
    body = json.loads(route.calls.last.request.content)
    elements = body["card"]["elements"]
    content = elements[0]["text"]["content"]
    assert result.ok and result.detail == "code=0"
    assert body["msg_type"] == "interactive"
    assert body["timestamp"] == "1790700000" and body["sign"] == "CX4RIQmjYTknzzq/eNcdTHe+fEBC6qxm8tpM81NnOLM="
    assert body["card"]["header"]["template"] == "red"
    assert body["card"]["header"]["title"]["content"].startswith("【S】美国9月非农")
    assert elements[0]["text"]["tag"] == "lark_md"
    assert "<at id=all></at>" in content and "2026-09-30 10:00:00" in content
    assert "金十快讯（另有 2 个来源）" in content
    # 资产行与正文都放在 plain_text 中，不进入 lark_md
    assert "BTC" not in content and "正文内容" not in content
    assert elements[1] == {"tag": "div", "text": {"tag": "plain_text", "content": "资产：BTC"}}
    assert elements[2] == {"tag": "div", "text": {"tag": "plain_text", "content": "正文内容"}}
    assert elements[3]["actions"][0]["url"] == "https://flash.jin10.com/detail/1"


@respx.mock
async def test_upgraded_a_card_without_secret(make_notifier):
    route = _ok_route()
    result = await make_notifier(secret=None).send_event(_card("A", upgraded=True, url=None), mention_all=False)
    body = json.loads(route.calls.last.request.content)
    assert result.ok and "sign" not in body and "timestamp" not in body
    assert body["card"]["header"]["template"] == "orange"
    assert body["card"]["header"]["title"]["content"].startswith("【升级】【A】")
    assert "<at id=all></at>" not in body["card"]["elements"][0]["text"]["content"]
    assert not _has_button(body)  # 没有原文链接时不加按钮
    assert len(body["card"]["elements"]) == 3  # 固定字段 + 资产行 + 正文


@respx.mock
async def test_error_classification(make_notifier):
    notifier = make_notifier()
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
async def test_digest_and_health_cards(make_notifier):
    route = _ok_route()
    notifier = make_notifier()
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


# ---------- 防注入 ----------


@respx.mock
async def test_event_card_untrusted_text_never_in_lark_md(make_notifier):
    """正文、资产含 @所有人 与钓鱼链接：lark_md 中不得出现，只出现在 plain_text 中"""
    route = _ok_route()
    card = _card(body=EVIL_BODY, assets=["BTC<at id=all></at>", "[领取](https://evil.example)"])
    await make_notifier().send_event(card, mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    for content in _lark_md(payload):
        assert "<at" not in content and "](" not in content and "BTC" not in content
    assert EVIL_BODY in _plain_text(payload)
    assert "资产：BTC<at id=all></at>、[领取](https://evil.example)" in _plain_text(payload)
    # lark_md 里只有代码生成的固定字段
    assert _lark_md(payload) == ["**时间**：2026-09-30 10:00:00\n**来源**：金十快讯"]


@respx.mock
async def test_event_card_mention_all_is_only_at_in_lark_md(make_notifier):
    """mention_all=True 时，唯一的 <at> 来自固定字段，不来自正文"""
    route = _ok_route()
    await make_notifier().send_event(_card(body=EVIL_BODY), mention_all=True)
    payload = json.loads(route.calls.last.request.content)
    assert "".join(_lark_md(payload)).count("<at") == 1
    assert _lark_md(payload) == ["<at id=all></at>\n**时间**：2026-09-30 10:00:00\n**来源**：金十快讯"]
    assert EVIL_BODY in _plain_text(payload)


def test_md_safe_replaces_markdown_special_chars():
    for char in "<>[]()*_~`":
        assert char not in feishu._md_safe(f"a{char}b")
    assert feishu._md_safe("BTC") == "BTC"
    assert "<" not in feishu._md_safe("[x](y)<at id=all></at>")
    assert "\n" not in feishu._md_safe("a\nb")


@respx.mock
async def test_digest_card_is_plain_text_only(make_notifier):
    route = _ok_route()
    evil = _card("A", title="<at id=all></at> [点此领取](https://evil.example)", url="https://evil.example/x")
    await make_notifier().send_digest([evil, _card("A", title="普通标题")])
    payload = json.loads(route.calls.last.request.content)
    assert _lark_md(payload) == []
    (div,) = payload["card"]["elements"]
    assert div["text"]["tag"] == "plain_text"
    lines = div["text"]["content"].split("\n")
    assert lines[0] == "10:00 [金十快讯] <at id=all></at> [点此领取](https://evil.example)"
    assert lines[1] == "10:00 [金十快讯] 普通标题"
    assert "evil.example/x" not in div["text"]["content"]  # 不再拼接原文链接


@respx.mock
async def test_health_card_detail_is_plain_text(make_notifier):
    route = _ok_route()
    detail = "<at id=all></at> [点此](https://evil.example) 连续失败"
    await make_notifier().send_health("jin10", False, detail)
    payload = json.loads(route.calls.last.request.content)
    assert _lark_md(payload) == []
    assert payload["card"]["elements"] == [{"tag": "div", "text": {"tag": "plain_text", "content": detail}}]


# ---------- 响应健壮性 ----------


@respx.mock
@pytest.mark.parametrize("raw", [b"[1]", b'"ok"', b"0", b"null"])
async def test_non_dict_json_response_is_not_retryable(make_notifier, raw):
    respx.post(HOOK).mock(return_value=httpx.Response(200, content=raw))
    result = await make_notifier().send_event(_card(), mention_all=False)
    assert result.ok is False and result.retryable is False
    assert result.detail.startswith("响应格式异常：")


@respx.mock
async def test_non_json_text_response_is_not_retryable(make_notifier):
    respx.post(HOOK).mock(return_value=httpx.Response(200, text="<html>not json</html>"))
    result = await make_notifier().send_event(_card(), mention_all=False)
    assert result.ok is False and result.retryable is False
    assert "响应不是 JSON" in result.detail


@respx.mock
async def test_unhashable_code_does_not_raise(make_notifier):
    respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": [1], "msg": "weird"}))
    result = await make_notifier().send_event(_card(), mention_all=False)
    assert result.ok is False and result.retryable is False


@respx.mock
async def test_http_429_is_retryable(make_notifier):
    respx.post(HOOK).mock(return_value=httpx.Response(429, text="too many requests"))
    result = await make_notifier().send_event(_card(), mention_all=False)
    assert (result.ok, result.retryable) == (False, True) and "429" in result.detail


# ---------- 客户端所有权 ----------


async def test_injected_client_is_not_closed_by_aclose():
    client = httpx.AsyncClient(trust_env=False)
    try:
        notifier = FeishuNotifier(HOOK, None, client=client)
        await notifier.aclose()
        assert client.is_closed is False
    finally:
        await client.aclose()


async def test_owned_client_is_closed_by_aclose():
    notifier = FeishuNotifier(HOOK, None)
    client = notifier._client
    assert notifier._owns_client is True and client.is_closed is False
    await notifier.aclose()
    assert client.is_closed is True


# ---------- 网络错误信息 ----------


@respx.mock
@pytest.mark.parametrize("exc", [httpx.ConnectError("连接失败"), httpx.ReadTimeout("读取超时")])
async def test_network_error_detail_contains_exception_type(make_notifier, exc):
    respx.post(HOOK).mock(side_effect=exc)
    result = await make_notifier().send_event(_card(), mention_all=False)
    assert (result.ok, result.retryable) == (False, True)
    assert result.detail == f"网络错误：{type(exc).__name__} {exc}"


# ---------- 资产行 ----------


@respx.mock
async def test_assets_line_is_plain_text_between_fixed_fields_and_body(make_notifier):
    route = _ok_route()
    await make_notifier().send_event(_card(assets=["BTC", "ETH"], body="正文内容"), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    elements = payload["card"]["elements"]
    assert [e["tag"] for e in elements] == ["div", "div", "div", "action"]
    assert elements[1]["text"] == {"tag": "plain_text", "content": "资产：BTC、ETH"}
    assert elements[2]["text"] == {"tag": "plain_text", "content": "正文内容"}
    assert "资产" not in "".join(_lark_md(payload)) and "BTC" not in "".join(_lark_md(payload))


@respx.mock
async def test_assets_line_omitted_when_no_assets(make_notifier):
    route = _ok_route()
    await make_notifier().send_event(_card(assets=[], body="正文内容"), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    assert _asset_lines(payload) == [] and "资产" not in "".join(_plain_text(payload))
    await make_notifier().send_event(_card(assets=[], body=""), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    assert [e["tag"] for e in payload["card"]["elements"]] == ["div", "action"]  # 只有固定字段与按钮


@respx.mock
@pytest.mark.parametrize(
    ("count", "expected_suffix"),
    [(1, ""), (10, ""), (11, " 等 11 个"), (25, " 等 25 个")],
)
async def test_assets_line_limited_to_ten(make_notifier, count, expected_suffix):
    route = _ok_route()
    assets = [f"A{i}" for i in range(count)]
    await make_notifier().send_event(_card(assets=assets), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    assert _asset_lines(payload) == ["资产：" + "、".join(assets[:10]) + expected_suffix]


# ---------- 原文按钮 url 校验 ----------


@respx.mock
@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(1)", "ftp://example.com/a", "flash.jin10.com/1", "", "   ",
        "https://", "http://", "https:///path", "https://@/x",  # 缺少主机
        "https://a b/c", "https://example.com/a b",  # 含空白
        "https://example.com/a\nb", "https://example.com/a\r\nb", "https://example.com/a\tb",  # 含换行、制表符
        "https://example.com/\x00", "https://exa\x1bmple.com/",  # 含控制字符
        "https://example.com/\u3000a",  # 含全角空白
        "https://[::1/a",  # urlsplit 会抛 ValueError
        "https://exa\u200bmple.com/a", "https://example.com/a\u200b",  # 含零宽空格（Unicode Cf 格式字符）
        "\ufeffhttps://example.com/a", "https://example.com\u2060/a", "https://example.com/\u202etxt.exe",  # BOM、词连接符、方向控制
        "https://example.com:99999/a", "https://example.com:abc/a", "https://example.com:-1/a",  # 非法端口
    ],
)
async def test_button_omitted_for_non_http_url(make_notifier, url):
    route = _ok_route()
    await make_notifier().send_event(_card(url=url), mention_all=False)
    assert not _has_button(json.loads(route.calls.last.request.content))


@respx.mock
@pytest.mark.parametrize(
    "url",
    ["http://example.com/a", "https://example.com/a", "HTTPS://Example.com/A", "https://example.com:8443/a?b=c&d=e#f",
     "https://example.com/新闻/1", "https://example.com:65535/a", "https://user:pw@example.com:8080/a"],
)
async def test_button_present_for_http_url(make_notifier, url):
    route = _ok_route()
    await make_notifier().send_event(_card(url=url), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    assert payload["card"]["elements"][-1]["actions"][0]["url"] == url


# ---------- 正文与标题去重 ----------


@respx.mock
@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("美国9月非农就业人口增加8.2万人：详细内容", "详细内容"),
        ("美国9月非农就业人口增加8.2万人 ， 详细内容", "详细内容"),
        ("美国9月非农就业人口增加8.2万人\n详细内容", "详细内容"),
        ("与标题无关的正文", "与标题无关的正文"),
    ],
)
async def test_body_dedup_strips_leading_title(make_notifier, body, expected):
    route = _ok_route()
    await make_notifier().send_event(_card(body=body), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    assert [e["text"]["content"] for e in _body_div(payload)] == [expected]


@respx.mock
@pytest.mark.parametrize(
    ("title", "body", "expected"),
    [
        # 标题只是正文开头的一部分（后面紧跟数字、文字）：保留完整正文，不能切成残缺的「25个基点」
        ("美联储降息", "美联储降息25个基点", "美联储降息25个基点"),
        ("美联储降息", "美联储降息了", "美联储降息了"),
        ("美联储降息", "美联储降息Fed", "美联储降息Fed"),
        # 标题后是标点或空白：去掉重复的标题前缀
        ("美联储降息", "美联储降息：幅度 25 个基点", "幅度 25 个基点"),
        ("美联储降息", "美联储降息 幅度 25 个基点", "幅度 25 个基点"),
    ],
)
async def test_body_dedup_respects_title_boundary(make_notifier, title, body, expected):
    route = _ok_route()
    await make_notifier().send_event(_card(title=title, body=body), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    assert [e["text"]["content"] for e in _body_div(payload)] == [expected]


@respx.mock
@pytest.mark.parametrize("suffix", ["", " ", "。", " ，。 "])
async def test_body_omitted_when_only_repeats_title(make_notifier, suffix):
    route = _ok_route()
    title = "美国9月非农就业人口增加8.2万人"
    await make_notifier().send_event(_card(title=title, body=title + suffix, assets=[]), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    assert _body_div(payload) == []
    # 标题只应出现在 header 中，不得在任何正文元素里重复
    assert title not in "".join(_lark_md(payload)) and _plain_text(payload) == [f"【S】{title}", "查看原文"]


@respx.mock
async def test_body_omitted_when_empty(make_notifier):
    route = _ok_route()
    await make_notifier().send_event(_card(body=""), mention_all=False)
    assert _body_div(json.loads(route.calls.last.request.content)) == []


# ---------- 截断加省略号 ----------


@respx.mock
async def test_title_truncated_with_ellipsis_over_80(make_notifier):
    route = _ok_route()
    notifier = make_notifier()
    await notifier.send_event(_card(title="标" * 100), mention_all=False)
    header = json.loads(route.calls.last.request.content)["card"]["header"]["title"]["content"]
    assert header == "【S】" + "标" * 80 + "…"
    await notifier.send_event(_card(title="标" * 80), mention_all=False)  # 恰好 80 字不截断
    header = json.loads(route.calls.last.request.content)["card"]["header"]["title"]["content"]
    assert header == "【S】" + "标" * 80


@respx.mock
async def test_body_truncated_with_ellipsis_over_300(make_notifier):
    route = _ok_route()
    await make_notifier().send_event(_card(body="正" * 400), mention_all=False)
    payload = json.loads(route.calls.last.request.content)
    assert [e["text"]["content"] for e in _body_div(payload)] == ["正" * 300 + "…"]


@respx.mock
async def test_digest_title_and_health_detail_truncated(make_notifier):
    route = _ok_route()
    notifier = make_notifier()
    await notifier.send_digest([_card("A", title="标" * 70)])
    line = json.loads(route.calls.last.request.content)["card"]["elements"][0]["text"]["content"]
    assert line == "10:00 [金十快讯] " + "标" * 60 + "…"
    await notifier.send_health("jin10", False, "详" * 600)
    detail = json.loads(route.calls.last.request.content)["card"]["elements"][0]["text"]["content"]
    assert detail == "详" * 500 + "…"
