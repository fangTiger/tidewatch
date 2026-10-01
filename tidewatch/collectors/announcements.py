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
_SUSPEND_WORDS = ("suspend", "suspension", "cease", "halt", "close", "pause", "暂停")
# 恢复/重新开放类词汇。完成类只用较窄的短语：宽泛的 complet / enabl 会误伤
# 「Suspend ... Until the Upgrade Is Completed」这类仍是暂停的公告
_RESTORE_WORDS = (
    "resum", "reopen", "restor", "enabled", "re-enable", "to enable", "will enable",
    "completed the", "completes the", "has completed", "have completed",
)

_PAREN = re.compile(r"\(([A-Z0-9]{2,15})\)")
# 币种代码可以数字开头（1INCH、1000PEPE）；纯数字（日期等）在 extract_symbols 中过滤
_TOKEN = re.compile(r"(?<![A-Za-z0-9])([A-Z0-9][A-Z0-9]{1,14})(?![A-Za-z0-9])")
_QUOTE_SUFFIXES = ("USDT", "USDC", "USD")
# 常见的非币种大写词（交易所名、法币、术语）
_STOP = {
    "OKX", "USD", "USDT", "USDC", "USDS", "API", "P2P", "NFT", "VIP", "UTC", "ETF", "FAQ", "KYC", "APR",
    "APY", "CEX", "DEX", "AMA", "OTC", "UAH", "EUR", "TRY", "BRL", "JPY", "IDR", "NGN", "ARS", "RUB",
    "GBP", "AUD", "PERP", "WEB3", "UID", "IPO", "FDUSD", "PYUSD", "SWAP",
    # 术语：双重验证、合规、代币发行相关
    "2FA", "USD1", "AML", "KYB", "TGE", "ICO", "IEO", "SEPA",
    # 更多法币代码
    "KRW", "CNY", "HKD", "CAD", "CHF", "SGD", "INR", "MXN", "PLN", "ZAR", "AED", "THB", "PHP", "VND",
}


def _first_index(lowered: str, words: tuple[str, ...]) -> int:
    """words 中任一词在 lowered 里最早出现的位置；都没有出现返回 -1"""
    positions = [pos for word in words if (pos := lowered.find(word)) >= 0]
    return min(positions) if positions else -1


def _is_restore(lowered: str) -> bool:
    """标题是否属于恢复类：先出现的动词优先

    恢复类先出现（或只出现恢复类）→ 恢复；暂停类先出现 → 暂停（即使后面有 resume / Completed，
    那只是说明何时恢复，如「Will Suspend Deposits ... Until the Upgrade Is Completed」）。
    """
    restore_pos = _first_index(lowered, _RESTORE_WORDS)
    if restore_pos < 0:
        return False
    suspend_pos = _first_index(lowered, _SUSPEND_WORDS)
    return suspend_pos < 0 or restore_pos < suspend_pos


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
    if hint == "delisting":
        return "perp_delisting" if perp else "delisting"
    if hint == "listing":
        if any(word in lowered for word in _LISTING_WORDS):
            return "perp_listing" if perp else "spot_listing"
        return "other"
    restore = _is_restore(lowered)
    if hint == "suspension":
        return "other" if restore else "deposit_withdraw_suspend"
    is_suspend = (
        ("deposit" in lowered or "withdraw" in lowered)
        and any(word in lowered for word in _SUSPEND_WORDS)
        and not restore
    )
    return "deposit_withdraw_suspend" if is_suspend else "other"


def _usable(symbols: list[str]) -> list[str]:
    """去重并过滤停用词、纯数字，保持出现顺序"""
    return [sym for sym in dict.fromkeys(symbols) if sym not in _STOP and not sym.isdigit()]


def extract_symbols(title: str) -> list[str]:
    """从英文标题中提取币种代码：优先取括号内代码，否则取大写词并去掉 USDT/USDC/USD 后缀

    括号内的代码是标题明示的，不做字母数限制；先过滤停用词，过滤后为空（如 (USDT)、(UTC)）再回退到正文大写词。
    代码可以数字开头（1INCH、1000PEPEUSDT → 1000PEPE）；纯数字、停用词（交易所名、法币、稳定币、术语）不算币种。
    数字开头的「裸词」去掉数字后至少要有 2 个字母（20X、24H、3L 不算）；带报价后缀（2ZUSDT）
    或后面紧跟 /（2Z/USDT）的词是交易对写法，不受该限制。
    """
    text = unicodedata.normalize("NFKC", title)
    found = _usable(_PAREN.findall(text))
    if found:
        return found
    candidates: list[str] = []
    for match in _TOKEN.finditer(text):
        token = match.group(1)
        if token in _STOP:  # 先排除停用词再去后缀：FDUSD、PYUSD 以 USD 结尾，去后缀后会变成 FD、PY
            continue
        suffixed = False
        for suffix in _QUOTE_SUFFIXES:
            if token.endswith(suffix) and len(token) > len(suffix) + 1:
                token = token[: -len(suffix)]
                suffixed = True
                break
        paired = text[match.end() : match.end() + 1] == "/"  # 2Z/USDT 这类交易对写法
        # 数字开头的裸词（20X、24H、3L 这类杠杆倍数、时间周期）去掉数字后至少要有 2 个字母才算币种
        if token[0].isdigit() and not suffixed and not paired and sum(ch.isalpha() for ch in token) < 2:
            continue
        candidates.append(token)
    return _usable(candidates)


def binance_dedupe_key(title: str, published_at: datetime) -> str:
    """币安公告去重键：规范化标题 + 发布日期（UTC），WS 与列表接口共用

    published_at 必须带时区：naive datetime 会被 astimezone 当成本地时间，换机器或时区后同一公告会得到不同的键。
    """
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        raise ValueError(f"发布时间必须带时区信息（收到 naive datetime：{published_at.isoformat()}）")
    day = published_at.astimezone(timezone.utc).date().isoformat()
    digest = hashlib.sha1(f"{normalize_title(title)}|{day}".encode("utf-8")).hexdigest()
    return f"binance_ann:{digest}"
