"""Hyperliquid 巨鲸名单管理命令行工具。"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path

from tidewatch.config import ConfigError, load_settings
from tidewatch.hyperliquid.registry import WalletRegistry, normalize_address
from tidewatch.store.db import create_db_engine


REPO_ROOT = Path(__file__).resolve().parents[2]

_TIER_NAMES = {"core": "核心", "focus": "重点", "candidate": "候选", None: "-"}
_SOURCE_NAMES = {"manual": "手动", "leaderboard": "排行榜", "discovered": "发现"}
_STYLE_NAMES = {
    "directional": "方向型",
    "swing": "波段",
    "market_maker": "做市",
    "high_frequency": "高频",
}


def _address(value: str) -> str:
    try:
        return normalize_address(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """解析名单管理子命令。"""
    parser = argparse.ArgumentParser(description="管理 Hyperliquid 巨鲸名单")
    commands = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (("add", "添加重点钱包"), ("remove", "移除手动钱包")):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("address", type=_address, help="0x 开头的 40 位十六进制地址")

    core = commands.add_parser("core", help="设置或解除核心钱包")
    core.add_argument("address", type=_address)
    core.add_argument("--remove", action="store_true", help="解除核心标记")

    exclude = commands.add_parser("exclude", help="设置或解除排除")
    exclude.add_argument("address", type=_address)
    exclude.add_argument("--remove", action="store_true", help="解除排除")

    label = commands.add_parser("label", help="设置用户备注")
    label.add_argument("address", type=_address)
    label.add_argument("note", help="备注文字；空字符串表示清除")

    listing = commands.add_parser("list", help="列出钱包")
    listing.add_argument("--tier", choices=("core", "focus", "candidate"), help="按层级筛选")
    listing.add_argument(
        "--excluded",
        choices=("all", "yes", "no"),
        default="all",
        help="按排除状态筛选（默认 all）",
    )
    return parser.parse_args(argv)


async def execute(args: argparse.Namespace, registry: WalletRegistry) -> str:
    """执行已解析命令并返回中文终端输出。"""
    command = args.command
    if command == "add":
        changed = await registry.add(args.address)
        if changed:
            return f"已添加重点钱包：{args.address}"
        return f"无需变更：{args.address} 已在手动名单中"
    if command == "remove":
        changed = await registry.remove(args.address)
        if changed:
            return f"已移除手动钱包：{args.address}"
        return f"无需变更：{args.address} 不是可移除的手动钱包"
    if command == "core":
        changed = await registry.set_core(args.address, not args.remove)
        if not changed:
            return f"无需变更：{args.address} 的核心状态未变化"
        action = "已解除核心钱包" if args.remove else "已设为核心钱包"
        return f"{action}：{args.address}"
    if command == "exclude":
        changed = await registry.set_excluded(args.address, not args.remove)
        if not changed:
            return f"无需变更：{args.address} 的排除状态未变化"
        action = "已解除排除" if args.remove else "已排除钱包"
        return f"{action}：{args.address}"
    if command == "label":
        changed = await registry.set_label(args.address, args.note)
        return f"已设置备注：{args.address}" if changed else f"无需变更：{args.address} 的备注未变化"
    if command == "list":
        excluded = None if args.excluded == "all" else args.excluded == "yes"
        rows = await registry.list_wallets(tier=args.tier, excluded=excluded)
        return _format_rows(rows)
    raise ValueError(f"未知命令：{command}")


def _format_rows(rows) -> str:
    header = "地址 | 备注/标签 | 来源 | 层级 | 排除 | 风格 | 当前持仓名义"
    divider = "--- | --- | --- | --- | --- | --- | ---:"
    lines = [header, divider]
    if not rows:
        lines.append("（无匹配钱包）")
        return "\n".join(lines)
    for row in rows:
        labels = " / ".join(row.external_labels)
        name = row.note or labels or "-"
        if row.note and labels:
            name = f"{row.note}（{labels}）"
        lines.append(
            " | ".join(
                (
                    row.address,
                    name,
                    _SOURCE_NAMES.get(row.source, row.source),
                    _TIER_NAMES.get(row.tier, row.tier or "-"),
                    "是" if row.excluded else "否",
                    _STYLE_NAMES.get(row.style, row.style or "-"),
                    f"${row.total_notional:,.2f}",
                )
            )
        )
    return "\n".join(lines)


async def _run(args: argparse.Namespace) -> str:
    settings = load_settings(REPO_ROOT)
    engine = create_db_engine(settings.secrets.database_url)
    registry = WalletRegistry(
        engine,
        configured_core=settings.hyperliquid.core_wallets,
        core_limit=settings.hyperliquid.tiers.core_limit,
        focus_limit=settings.hyperliquid.tiers.focus_limit,
    )
    try:
        return await execute(args, registry)
    finally:
        await engine.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    """运行巨鲸名单管理工具。"""
    args = parse_args(argv)
    try:
        output = asyncio.run(_run(args))
    except (ConfigError, ValueError) as exc:
        print(f"操作失败：{exc}", file=sys.stderr)
        return 2
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
