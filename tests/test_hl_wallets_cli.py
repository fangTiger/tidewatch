from __future__ import annotations

import pytest
from sqlalchemy import update

from fakes import async_timeout
from tidewatch.hyperliquid.registry import WalletRegistry
from tidewatch.store.tables import hl_wallets
from tidewatch.tools.hl_wallets import execute, parse_args


def _address(number: int) -> str:
    return f"0x{number:040x}"


def test_parse_args_supports_all_commands_and_normalizes_address(capsys):
    upper = "0x" + "AB" * 20
    assert parse_args(["add", upper]).address == upper.lower()
    assert parse_args(["remove", upper]).command == "remove"
    assert parse_args(["core", upper, "--remove"]).remove is True
    assert parse_args(["exclude", upper, "--remove"]).remove is True
    assert parse_args(["label", upper, "测试备注"]).note == "测试备注"
    listed = parse_args(["list", "--tier", "focus", "--excluded", "no"])
    assert listed.tier == "focus" and listed.excluded == "no"

    with pytest.raises(SystemExit):
        parse_args(["add", "0x1234"])
    assert "0x 开头的 40 位十六进制" in capsys.readouterr().err


@async_timeout()
async def test_execute_crud_and_chinese_list_output(engine, clock):
    address = _address(1)
    registry = WalletRegistry(engine, clock=clock)

    assert await execute(parse_args(["add", address]), registry) == f"已添加重点钱包：{address}"
    assert "无需变更" in await execute(parse_args(["add", address]), registry)
    assert await execute(parse_args(["label", address, "测试巨鲸"]), registry) == f"已设置备注：{address}"
    async with engine.begin() as conn:
        await conn.execute(
            update(hl_wallets).where(hl_wallets.c.address == address).values(style="directional")
        )
    listing = await execute(parse_args(["list", "--tier", "focus"]), registry)
    assert "地址 | 备注/标签 | 来源 | 层级 | 排除 | 风格 | 当前持仓名义" in listing
    assert address in listing and "测试巨鲸" in listing and "手动" in listing and "重点" in listing
    assert "方向型" in listing and "directional" not in listing

    assert await execute(parse_args(["core", address]), registry) == f"已设为核心钱包：{address}"
    assert "核心" in await execute(parse_args(["list", "--tier", "core"]), registry)
    assert await execute(parse_args(["core", address, "--remove"]), registry) == f"已解除核心钱包：{address}"
    assert await execute(parse_args(["exclude", address]), registry) == f"已排除钱包：{address}"
    assert "是" in await execute(parse_args(["list", "--excluded", "yes"]), registry)
    assert await execute(parse_args(["exclude", address, "--remove"]), registry) == f"已解除排除：{address}"
    assert await execute(parse_args(["remove", address]), registry) == f"已移除手动钱包：{address}"
    assert "（无匹配钱包）" in await execute(parse_args(["list"]), registry)
