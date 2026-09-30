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
    """把带时区的时间格式化为上海时间字符串"""
    return when.astimezone(LOCAL_TZ).strftime(fmt)
