"""向飞书群发送一组联调消息，验证机器人配置与正式卡片样式。"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from tidewatch.clock import LOCAL_TZ
from tidewatch.config import ConfigError, Settings, load_settings
from tidewatch.notify.base import EventCard, SendResult
from tidewatch.notify.feishu import FeishuNotifier

REPO_ROOT = Path(__file__).resolve().parents[2]
ALL_GROUPS = frozenset({"s", "a", "digest", "health"})
_TEST_PREFIX = "【联调测试】"
_URL_PATTERN = re.compile(r"https?://[^\s）)]+")


@dataclass(frozen=True)
class _CheckItem:
    """一项待发送的联调检查。"""

    group: str
    label: str
    send: Callable[[], Awaitable[SendResult]]


def _event_card(
    card_id: int,
    *,
    level: str,
    title: str,
    body: str,
    source: str,
    occurred_at: datetime,
    url: str,
    assets: list[str],
    source_count: int = 1,
    upgraded: bool = False,
) -> EventCard:
    """按正式推送所用的领域结构构造联调事件。"""
    return EventCard(
        push_id=card_id,
        cluster_id=card_id,
        level=level,
        title=f"{_TEST_PREFIX}{title}",
        source=source,
        occurred_at=occurred_at,
        url=url,
        assets=assets,
        body=body,
        source_count=source_count,
        upgraded=upgraded,
    )


def _build_cards(now: datetime) -> tuple[EventCard, EventCard, EventCard, list[EventCard]]:
    """构造事件卡片及三项 A 级汇总数据。"""
    s_card = _event_card(
        1,
        level="S",
        title="美国非农数据大幅超出市场预期",
        body="示例：重要宏观数据发布，可能引发全球市场显著波动。",
        source="jin10",
        occurred_at=now,
        url="https://www.jin10.com/",
        assets=["BTC", "ETH", "USD"],
        source_count=3,
    )
    a_card = _event_card(
        2,
        level="A",
        title="比特币突破关键价格区间",
        body="示例：比特币价格出现较大波动，请关注后续行情变化。",
        source="jin10",
        occurred_at=now,
        url="https://www.jin10.com/",
        assets=["BTC"],
    )
    upgraded_card = _event_card(
        3,
        level="A",
        title="交易所公告影响范围扩大",
        body="示例：同一事件出现新的重要来源，现按升级样式再次推送。",
        source="binance_ann",
        occurred_at=now,
        url="https://www.binance.com/zh-CN/support/announcement",
        assets=["BTC", "ETH"],
        source_count=2,
        upgraded=True,
    )
    digest_cards = [
        a_card,
        _event_card(
            4,
            level="A",
            title="以太坊市场活跃度上升",
            body="示例汇总项目。",
            source="jin10",
            occurred_at=now,
            url="https://www.jin10.com/",
            assets=["ETH"],
        ),
        _event_card(
            5,
            level="A",
            title="OKX 发布资产相关公告",
            body="示例汇总项目。",
            source="okx_ann",
            occurred_at=now,
            url="https://www.okx.com/zh-hans/help/section/announcements-latest-announcements",
            assets=["BTC"],
        ),
    ]
    return s_card, a_card, upgraded_card, digest_cards


def _sanitize_detail(detail: str, webhook: str, secret: str | None) -> str:
    """从终端错误详情中移除敏感信息，并折叠换行以保持单行输出。"""
    sanitized = detail.replace(webhook, "[已脱敏]")
    if secret:
        sanitized = sanitized.replace(secret, "[已脱敏]")
    sanitized = _URL_PATTERN.sub("[已脱敏地址]", sanitized)
    return " ".join(sanitized.splitlines())


async def run_checks(
    settings: Settings,
    groups: frozenset[str],
    *,
    now: datetime | None = None,
) -> int:
    """执行选中的联调项，逐项输出结果并返回进程退出码。"""
    webhook = settings.secrets.feishu_webhook
    if not webhook:
        print(
            "未配置飞书机器人：请在 .env 中填写 FEISHU_WEBHOOK，建议同时填写 FEISHU_SECRET。",
            file=sys.stderr,
        )
        return 2
    secret = settings.secrets.feishu_secret
    if not secret:
        print(
            "警告：未配置 FEISHU_SECRET，将不附加签名；建议在飞书机器人中开启签名校验。",
            file=sys.stderr,
        )

    current = now or datetime.now(LOCAL_TZ)
    s_card, a_card, upgraded_card, digest_cards = _build_cards(current)
    notifier = FeishuNotifier(webhook, secret)
    items = [
        _CheckItem("s", "S 级卡片", lambda: notifier.send_event(s_card, mention_all=True)),
        _CheckItem("a", "A 级卡片", lambda: notifier.send_event(a_card, mention_all=False)),
        _CheckItem("a", "A 级升级卡片", lambda: notifier.send_event(upgraded_card, mention_all=False)),
        _CheckItem("digest", "A 级汇总", lambda: notifier.send_digest(digest_cards)),
        _CheckItem(
            "health",
            "健康异常告警",
            lambda: notifier.send_health(f"{_TEST_PREFIX}金十快讯", False, "模拟采集器连续请求失败"),
        ),
        _CheckItem(
            "health",
            "健康恢复通知",
            lambda: notifier.send_health(f"{_TEST_PREFIX}金十快讯", True, "模拟采集器已恢复正常"),
        ),
    ]
    selected = [item for item in items if item.group in groups]
    successes = 0
    try:
        for item in selected:
            try:
                result = await item.send()
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                print(f"{item.label}：失败（{_sanitize_detail(detail, webhook, secret)}）")
                continue
            if result.ok:
                successes += 1
                print(f"{item.label}：成功")
            else:
                detail = _sanitize_detail(result.detail, webhook, secret)
                print(f"{item.label}：失败（{detail}）")
    finally:
        await notifier.aclose()

    print(f"共 {len(selected)} 项，成功 {successes} 项")
    return 0 if successes == len(selected) else 1


def _groups(value: str) -> frozenset[str]:
    """解析逗号分隔的联调分组。"""
    groups = frozenset(part.strip().lower() for part in value.split(",") if part.strip())
    unknown = groups - ALL_GROUPS
    if not groups:
        raise argparse.ArgumentTypeError("至少指定一个分组")
    if unknown:
        raise argparse.ArgumentTypeError(f"未知分组：{', '.join(sorted(unknown))}")
    return groups


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """解析飞书联调工具参数。"""
    parser = argparse.ArgumentParser(description="发送飞书机器人联调消息")
    parser.add_argument(
        "--only",
        type=_groups,
        default=ALL_GROUPS,
        metavar="s,a,digest,health",
        help="仅发送指定分组，多个分组用逗号分隔",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """加载仓库配置并运行飞书联调。"""
    args = parse_args(argv)
    try:
        settings = load_settings(REPO_ROOT)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2
    return asyncio.run(run_checks(settings, args.only))


if __name__ == "__main__":
    raise SystemExit(main())
