"""交易所公告的通用处理：分类、标题币种提取、币安去重键"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import datetime, timezone

from tidewatch.pipeline.normalize import normalize_title

_PERP_WORDS = ("perpetual", "futures", "perp", "swap")
_LISTING_WORDS = ("will list", "to list", "will launch", "lists ", "listing of")
_SUSPEND_WORDS = ("suspend", "suspension", "cease", "halt", "close")

_PAREN = re.compile(r"\(([A-Z0-9]{2,15})\)")
_TOKEN = re.compile(r"(?<![A-Za-z0-9])([A-Z][A-Z0-9]{1,14})(?![A-Za-z0-9])")
_QUOTE_SUFFIXES = ("USDT", "USDC", "USD")
# 常见的非币种大写词（交易所名、法币、术语）
_STOP = {
    "OKX", "USD", "USDT", "USDC", "USDS", "API", "P2P", "NFT", "VIP", "UTC", "ETF", "FAQ", "KYC", "APR",
    "APY", "CEX", "DEX", "AMA", "OTC", "UAH", "EUR", "TRY", "BRL", "JPY", "IDR", "NGN", "ARS", "RUB",
    "GBP", "AUD", "PERP", "WEB3", "UID", "IPO",
}


def classify(title: str, hint: str | None) -> str:
    """公告分类

    Args:
        title: 英文标题
        hint: 交易所栏目给出的大类：listing / delisting / suspension / None
    Returns:
        spot_listing / perp_listing / delisting / perp_delisting / deposit_withdraw_suspend / other
    """
    lowered = title.lower()
    perp = any(word in lowered for word in _PERP_WORDS)
    if hint == "delisting":
        return "perp_delisting" if perp else "delisting"
    if hint == "listing":
        if any(word in lowered for word in _LISTING_WORDS):
            return "perp_listing" if perp else "spot_listing"
        return "other"
    if hint == "suspension":
        return "other" if "resum" in lowered else "deposit_withdraw_suspend"
    is_suspend = (
        ("deposit" in lowered or "withdraw" in lowered)
        and any(word in lowered for word in _SUSPEND_WORDS)
        and "resum" not in lowered
    )
    return "deposit_withdraw_suspend" if is_suspend else "other"


def extract_symbols(title: str) -> list[str]:
    """从英文标题中提取币种代码：优先取括号内代码，否则取大写词并去掉 USDT/USDC/USD 后缀"""
    text = unicodedata.normalize("NFKC", title)
    found = _PAREN.findall(text)
    if not found:
        for token in _TOKEN.findall(text):
            for suffix in _QUOTE_SUFFIXES:
                if token.endswith(suffix) and len(token) > len(suffix) + 1:
                    token = token[: -len(suffix)]
                    break
            found.append(token)
    return [sym for sym in dict.fromkeys(found) if sym not in _STOP and not sym.isdigit()]


def binance_dedupe_key(title: str, published_at: datetime) -> str:
    """币安公告去重键：规范化标题 + 发布日期（UTC），WS 与列表接口共用"""
    day = published_at.astimezone(timezone.utc).date().isoformat()
    digest = hashlib.sha1(f"{normalize_title(title)}|{day}".encode("utf-8")).hexdigest()
    return f"binance_ann:{digest}"
