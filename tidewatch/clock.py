"""时间工具：统一获取 UTC 当前时间，组件通过注入 clock 便于测试"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable
from zoneinfo import ZoneInfo

Clock = Callable[[], datetime]

# 页面、日志、推送展示使用的本地时区
LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def utcnow() -> datetime:
    """返回带时区的 UTC 当前时间"""
    return datetime.now(timezone.utc)


def format_local(when: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """把带时区的时间格式化为上海时间字符串

    注意：when 必须是带时区的 datetime。不带时区的时间会被 astimezone
    按运行机器的系统时区解释，得到错误结果，因此调用方应先补齐时区（如 UTC）。
    """
    return when.astimezone(LOCAL_TZ).strftime(fmt)
