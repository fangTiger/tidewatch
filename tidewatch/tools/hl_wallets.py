"""Hyperliquid 巨鲸名单管理命令行工具。"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

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
    profile = commands.add_parser("profile", help="立即计算并显示单钱包画像")
    profile.add_argument("address", type=_address)
    return parser.parse_args(argv)


async def execute(
    args: argparse.Namespace,
    registry: WalletRegistry,
    *,
    profiler: Any = None,
) -> str:
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
    if command == "profile":
        if profiler is None:
            raise ValueError("画像服务未初始化")
        result = await profiler.profile_wallet(args.address)
        return _format_profile(result)
    raise ValueError(f"未知命令：{command}")


def _format_rows(rows) -> str:
    header = (
        "地址 | 备注/标签 | 来源 | 层级 | 排除 | 当前持仓名义 | "
        "账户价值 | 30 天盈亏 | 胜率 | 风格 | 画像时间"
    )
    divider = "--- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---"
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
                    f"${row.total_notional:,.2f}",
                    _money(row.profile.get("account_value")),
                    _money(row.profile.get("pnl_30d")),
                    _percent(row.profile.get("win_rate")),
                    _STYLE_NAMES.get(row.style, row.style or "-"),
                    row.profile_updated_at.isoformat() if row.profile_updated_at else "-",
                )
            )
        )
    return "\n".join(lines)


def _format_profile(result: dict[str, Any]) -> str:
    profile = dict(result.get("profile") or {})
    style = result.get("style")
    coverage = profile.get("coverage") if isinstance(profile.get("coverage"), dict) else {}
    lines = [
        f"地址：{result.get('address', '-')}",
        f"风格：{_STYLE_NAMES.get(style, style or '未知')}",
        f"画像时间：{_time_text(result.get('profile_updated_at') or profile.get('generated_at'))}",
        f"账户价值：{_money(profile.get('account_value'))}{_sample_suffix(coverage, 'account_value')}",
        f"总盈亏：{_money(profile.get('total_pnl'))}{_sample_suffix(coverage, 'total_pnl')}",
        f"30 天盈亏：{_money(profile.get('pnl_30d'))}{_sample_suffix(coverage, 'pnl_30d')}",
        f"总成交量：{_money(profile.get('volume_total'))}{_sample_suffix(coverage, 'volume_total')}",
        f"30 天成交量：{_money(profile.get('volume_30d'))}{_sample_suffix(coverage, 'volume_30d')}",
        f"最大回撤：{_money(profile.get('max_drawdown'))} / {_percent(profile.get('max_drawdown_ratio'))}{_sample_suffix(coverage, 'max_drawdown')}",
        f"胜率：{_percent(profile.get('win_rate'))}{_sample_suffix(coverage, 'win_rate')}",
        f"盈亏比：{_number(profile.get('profit_loss_ratio'))}{_sample_suffix(coverage, 'profit_loss_ratio')}",
        f"平均持仓：{_duration(profile.get('holding_duration_avg_s'))}{_sample_suffix(coverage, 'holding_duration')}",
        f"中位持仓：{_duration(profile.get('holding_duration_median_s'))}{_sample_suffix(coverage, 'holding_duration')}",
        f"未平轮次：{profile.get('open_rounds', '-')}",
        f"偏好币种：{_preferred_coins(profile.get('preferred_coins'))}{_sample_suffix(coverage, 'preferred_coins')}",
        f"日均成交：{_number(profile.get('daily_fill_count'))} 笔{_sample_suffix(coverage, 'daily_fill_count')}",
        f"挂单占比：{_percent(profile.get('passive_fill_ratio'))}",
        f"实际杠杆：{_number(profile.get('actual_leverage'))}x{_sample_suffix(coverage, 'actual_leverage')}",
        f"设置杠杆：{_configured_leverage(profile.get('configured_leverage'), profile.get('configured_leverage_notionals'))}{_sample_suffix(coverage, 'configured_leverage')}",
    ]
    return "\n".join(lines)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _money(value: Any) -> str:
    parsed = _decimal(value)
    return "数据不足" if parsed is None else f"${parsed:,.2f}"


def _number(value: Any) -> str:
    parsed = _decimal(value)
    return "数据不足" if parsed is None else f"{parsed:,.4f}".rstrip("0").rstrip(".")


def _percent(value: Any) -> str:
    parsed = _decimal(value)
    return "数据不足" if parsed is None else f"{parsed * 100:.2f}%"


def _duration(value: Any) -> str:
    parsed = _decimal(value)
    if parsed is None:
        return "数据不足"
    hours = parsed / Decimal(3600)
    return f"{hours:.2f} 小时"


def _preferred_coins(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "数据不足"
    return "、".join(
        f"{item.get('coin')}（{_money(item.get('notional_usd'))}）"
        for item in value
        if isinstance(item, dict)
    ) or "数据不足"


def _configured_leverage(value: Any, notionals: Any = None) -> str:
    if not isinstance(value, dict) or not value:
        return "数据不足"
    notional_values = notionals if isinstance(notionals, dict) else {}
    items = sorted(
        value.items(),
        key=lambda item: (
            -(_decimal(notional_values.get(item[0])) or Decimal(0)),
            item[0],
        ),
    )
    shown = "、".join(f"{coin} {leverage}x" for coin, leverage in items[:10])
    remaining = len(items) - 10
    return f"{shown}（另有 {remaining} 个）" if remaining > 0 else shown


def _sample_suffix(coverage: dict[str, Any], key: str) -> str:
    detail = coverage.get(key)
    if not isinstance(detail, dict):
        return ""
    parts = [f"样本量：{detail.get('sample_size', 0)}"]
    if detail.get("complete") is False:
        parts.append("历史不完整")
    if not detail.get("sufficient", True) and detail.get("reason"):
        parts.append(str(detail["reason"]))
    start = detail.get("start_at")
    end = detail.get("end_at")
    if start or end:
        parts.append(f"窗口：{start or '?'} 至 {end or '?'}")
    return f"（{'；'.join(parts)}）"


def _time_text(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value or "-")


async def _run(args: argparse.Namespace) -> str:
    settings = load_settings(REPO_ROOT)
    engine = create_db_engine(settings.secrets.database_url)
    registry = WalletRegistry(
        engine,
        configured_core=settings.hyperliquid.core_wallets,
        core_limit=settings.hyperliquid.tiers.core_limit,
        focus_limit=settings.hyperliquid.tiers.focus_limit,
    )
    runtime = None
    try:
        if args.command == "profile":
            from tidewatch.hyperliquid.runtime import HyperliquidRuntime

            runtime = HyperliquidRuntime(
                settings.hyperliquid,
                _CliHealth(),
                engine=engine,
                proxy_url=settings.proxy_url,
            )
            return await execute(args, registry, profiler=runtime.profiler)
        return await execute(args, registry)
    finally:
        if runtime is not None:
            await runtime.close_resources()
        await engine.dispose()


class _CliHealth:
    """单次画像命令不写健康状态。"""

    async def success(self, name: str) -> None:
        return None

    async def failure(self, name: str, error: str) -> None:
        return None


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
