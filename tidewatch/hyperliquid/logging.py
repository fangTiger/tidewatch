"""Hyperliquid 日志脱敏工具。"""

from __future__ import annotations

import re


_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")
_SENSITIVE_SUFFIX_RE = re.compile(r"\s*\[(?:SQL|parameters?)\s*:[\s\S]*$", re.IGNORECASE)


def short_address(address: str) -> str:
    """把完整钱包地址缩写为可识别但不可直接复用的形式。"""
    return f"{address[:6]}…{address[-4:]}" if _ADDRESS_RE.fullmatch(address) else redact_text(address)


def redact_text(value: object) -> str:
    """替换文本中的完整钱包地址，并移除 SQL 与参数尾部。"""
    text = _ADDRESS_RE.sub(lambda match: short_address(match.group(0)), str(value))
    return _SENSITIVE_SUFFIX_RE.sub("", text).strip()


def exception_summary(exc: BaseException) -> str:
    """返回不含 traceback、完整地址或 SQL 参数的异常摘要。"""
    reason = redact_text(exc)
    name = type(exc).__name__
    if not reason:
        return f"{name}()"
    return f"{name}: {reason[:500]}"
