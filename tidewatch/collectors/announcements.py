"""交易所公告的通用处理：分类、标题币种提取、币安去重键"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import datetime, timezone

from tidewatch.pipeline.normalize import normalize_title

# 合约关键词按单词边界匹配：避免 Uniswap、PancakeSwap、SushiSwap 这类币名里的 swap 被当成合约
_PERP_RE = re.compile(r"\b(?:perps?|perpetuals?|futures?|swaps?)\b", re.IGNORECASE)
_LISTING_WORDS = ("will list", "to list", "will launch", "lists ", "listing of")
_SUSPEND_WORDS = ("suspend", "suspension", "cease", "halt", "close")
# 出现这些词说明是恢复/重新开放类公告，不算暂停充提
_RESTORE_WORDS = ("resum", "reopen", "enabl", "restor", "complet")

_PAREN = re.compile(r"\(([A-Z0-9]{2,15})\)")
# 币种代码可以数字开头（1INCH、1000PEPE）；纯数字（日期等）在 extract_symbols 中过滤
_TOKEN = re.compile(r"(?<![A-Za-z0-9])([A-Z0-9][A-Z0-9]{1,14})(?![A-Za-z0-9])")
_QUOTE_SUFFIXES = ("USDT", "USDC", "USD")
# 常见的非币种大写词（交易所名、法币、术语）
_STOP = {
    "OKX", "USD", "USDT", "USDC", "USDS", "API", "P2P", "NFT", "VIP", "UTC", "ETF", "FAQ", "KYC", "APR",
    "APY", "CEX", "DEX", "AMA", "OTC", "UAH", "EUR", "TRY", "BRL", "JPY", "IDR", "NGN", "ARS", "RUB",
    "GBP", "AUD", "PERP", "WEB3", "UID", "IPO", "FDUSD", "PYUSD", "SWAP",
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
    perp = _PERP_RE.search(title) is not None
    restore = any(word in lowered for word in _RESTORE_WORDS)
    if hint == "delisting":
        return "perp_delisting" if perp else "delisting"
    if hint == "listing":
        if any(word in lowered for word in _LISTING_WORDS):
            return "perp_listing" if perp else "spot_listing"
        return "other"
    if hint == "suspension":
        return "other" if restore else "deposit_withdraw_suspend"
    is_suspend = (
        ("deposit" in lowered or "withdraw" in lowered)
        and any(word in lowered for word in _SUSPEND_WORDS)
        and not restore
    )
    return "deposit_withdraw_suspend" if is_suspend else "other"


def extract_symbols(title: str) -> list[str]:
    """从英文标题中提取币种代码：优先取括号内代码，否则取大写词并去掉 USDT/USDC/USD 后缀

    代码可以数字开头（1INCH、1000PEPEUSDT → 1000PEPE），但去掉数字后至少要有 2 个字母（20X、24H、3L 不算）；
    纯数字、停用词（交易所名、法币、稳定币、术语）不算币种。括号内的代码是标题明示的，不做字母数限制。
    """
    text = unicodedata.normalize("NFKC", title)
    found = _PAREN.findall(text)
    if not found:
        for token in _TOKEN.findall(text):
            if token in _STOP:  # 先排除停用词再去后缀：FDUSD、PYUSD 以 USD 结尾，去后缀后会变成 FD、PY
                continue
            for suffix in _QUOTE_SUFFIXES:
                if token.endswith(suffix) and len(token) > len(suffix) + 1:
                    token = token[: -len(suffix)]
                    break
            # 数字开头的词（20X、24H、3L 这类杠杆倍数、时间周期）去掉数字后至少要有 2 个字母才算币种
            if token[0].isdigit() and sum(ch.isalpha() for ch in token) < 2:
                continue
            found.append(token)
    return [sym for sym in dict.fromkeys(found) if sym not in _STOP and not sym.isdigit()]


def binance_dedupe_key(title: str, published_at: datetime) -> str:
    """币安公告去重键：规范化标题 + 发布日期（UTC），WS 与列表接口共用"""
    day = published_at.astimezone(timezone.utc).date().isoformat()
    digest = hashlib.sha1(f"{normalize_title(title)}|{day}".encode("utf-8")).hexdigest()
    return f"binance_ann:{digest}"
