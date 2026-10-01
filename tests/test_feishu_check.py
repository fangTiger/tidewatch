from __future__ import annotations

import asyncio
import json
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest
import respx

from tidewatch.tools import feishu_check


HOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/feishu-check-test-token"
SECRET = "feishu-check-test-secret"
WHEN = datetime(2026, 10, 1, 12, 34, 56, tzinfo=ZoneInfo("Asia/Shanghai"))


def _settings(webhook: str | None = HOOK, secret: str | None = SECRET):
    """构造联调工具所需的最小配置对象。"""
    return SimpleNamespace(secrets=SimpleNamespace(feishu_webhook=webhook, feishu_secret=secret))


def _payloads(route: respx.Route) -> list[dict]:
    """读取模拟飞书接口收到的 JSON 请求体。"""
    return [json.loads(call.request.content) for call in route.calls]


@respx.mock
async def test_run_sends_all_expected_signed_cards(capsys):
    """默认发送六项，复用正式卡片类型、签名和 @所有人行为。"""
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0, "msg": "success"}))

    exit_code = await asyncio.wait_for(
        feishu_check.run_checks(_settings(), feishu_check.ALL_GROUPS, now=WHEN),
        timeout=2,
    )

    payloads = _payloads(route)
    assert exit_code == 0
    assert len(payloads) == 6
    assert [item["card"]["header"]["template"] for item in payloads] == [
        "red",
        "orange",
        "orange",
        "orange",
        "red",
        "green",
    ]
    assert all(item["msg_type"] == "interactive" for item in payloads)
    assert all("timestamp" in item and "sign" in item for item in payloads)
    assert "<at id=all></at>" in payloads[0]["card"]["elements"][0]["text"]["content"]
    assert "<at id=all></at>" not in payloads[1]["card"]["elements"][0]["text"]["content"]
    assert payloads[2]["card"]["header"]["title"]["content"].startswith("【升级】【A】【联调测试】")
    assert "（3 条）" in payloads[3]["card"]["header"]["title"]["content"]
    assert all(
        "【联调测试】" in item["card"]["header"]["title"]["content"]
        for item in payloads[4:]
    )
    assert all("【联调测试】" in json.dumps(item, ensure_ascii=False) for item in payloads)
    output = capsys.readouterr()
    assert output.err == ""
    assert "S 级卡片：成功" in output.out
    assert "健康恢复通知：成功" in output.out
    assert output.out.rstrip().endswith("共 6 项，成功 6 项")
    assert HOOK not in output.out and SECRET not in output.out


@respx.mock
async def test_only_selects_requested_groups(capsys):
    """--only 的组按固定顺序展开，A 与健康组各包含两项。"""
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0}))

    exit_code = await asyncio.wait_for(
        feishu_check.run_checks(_settings(), frozenset({"a", "health"}), now=WHEN),
        timeout=2,
    )

    payloads = _payloads(route)
    assert exit_code == 0
    assert len(payloads) == 4
    assert [item["card"]["header"]["template"] for item in payloads] == ["orange", "orange", "red", "green"]
    assert capsys.readouterr().out.rstrip().endswith("共 4 项，成功 4 项")


@respx.mock
async def test_failure_returns_one_and_redacts_sensitive_values(capsys):
    """业务失败返回 1，且飞书详情中的 webhook 和密钥不会泄露。"""
    route = respx.post(HOOK).mock(
        return_value=httpx.Response(
            200,
            json={"code": 19021, "msg": f"bad {HOOK}\nsecret={SECRET}"},
        )
    )

    exit_code = await asyncio.wait_for(
        feishu_check.run_checks(_settings(), frozenset({"s"}), now=WHEN),
        timeout=2,
    )

    assert len(route.calls) == 1
    assert exit_code == 1
    output = capsys.readouterr().out
    assert "S 级卡片：失败（code=19021" in output
    assert "共 1 项，成功 0 项" in output
    assert HOOK not in output and SECRET not in output
    assert "[已脱敏]" in output
    assert len(output.splitlines()) == 2


def test_main_without_webhook_returns_two(monkeypatch, capsys):
    """未配置 webhook 时给出 .env 配置提示并返回 2。"""
    monkeypatch.setattr(feishu_check, "load_settings", lambda _path: _settings(webhook=None, secret=None))

    exit_code = feishu_check.main([])

    assert exit_code == 2
    output = capsys.readouterr()
    assert "FEISHU_WEBHOOK" in output.err
    assert ".env" in output.err
    assert "FEISHU_SECRET" in output.err
    assert output.out == ""


@respx.mock
async def test_webhook_without_secret_warns_and_continues(capsys):
    """未配置签名密钥时告警但仍发送，且请求体不带签名字段。"""
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0}))

    exit_code = await asyncio.wait_for(
        feishu_check.run_checks(_settings(secret=None), frozenset({"digest"}), now=WHEN),
        timeout=2,
    )

    assert exit_code == 0
    assert len(route.calls) == 1
    assert "sign" not in _payloads(route)[0]
    output = capsys.readouterr()
    assert "警告" in output.err and "FEISHU_SECRET" in output.err
    assert output.out.rstrip().endswith("共 1 项，成功 1 项")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("s,a,digest,health", frozenset({"s", "a", "digest", "health"})),
        (" health, s ", frozenset({"health", "s"})),
    ],
)
def test_parse_only(raw, expected):
    assert feishu_check.parse_args(["--only", raw]).only == expected


def test_parse_only_rejects_unknown_group():
    with pytest.raises(SystemExit) as exc_info:
        feishu_check.parse_args(["--only", "s,unknown"])
    assert exc_info.value.code == 2
