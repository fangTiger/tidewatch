"""规范化工具：HTML 清理、标题规范化、标题相似度、关注资产识别"""

from __future__ import annotations

import html
import re
import unicodedata

from tidewatch.config import AssetCfg

_TAG = re.compile(r"<[^>]+>")
_NON_WORD = re.compile(r"[\W_]+", re.UNICODE)


def clean_text(text: str | None) -> str:
    """去掉 HTML 标签与实体，合并多余空白"""
    no_tags = _TAG.sub(" ", text or "")
    return " ".join(html.unescape(no_tags).split())


def normalize_title(text: str | None) -> str:
    """规范化：NFKC（全角转半角）、英文小写、去掉空白与标点"""
    return _NON_WORD.sub("", unicodedata.normalize("NFKC", text or "").lower())


def contains_symbol(text: str | None, symbol: str) -> bool:
    """文本中是否以独立形式出现币种代码（区分大小写；前后不紧挨字母或数字，紧挨中文算独立）"""
    if not symbol:
        return False
    content = unicodedata.normalize("NFKC", text or "")
    return re.search(rf"(?<![A-Za-z0-9]){re.escape(symbol)}(?![A-Za-z0-9])", content) is not None


def _bigrams(text: str | None) -> set[str]:
    norm = normalize_title(text)
    if len(norm) < 2:
        return {norm} if norm else set()
    return {norm[i : i + 2] for i in range(len(norm) - 1)}


def title_similarity(a: str | None, b: str | None) -> float:
    """两个标题的字符二元组 Jaccard 相似度（0~1）"""
    grams_a, grams_b = _bigrams(a), _bigrams(b)
    union = grams_a | grams_b
    return len(grams_a & grams_b) / len(union) if union else 0.0


class AssetMatcher:
    """根据关注资产的代码与别名在文本中识别资产

    - 全大写英文（如 BTC、SOL）：区分大小写，且前后不能紧挨字母或数字
    - 其他英文别名（如 Bitcoin）：不区分大小写，同样要求前后不紧挨字母或数字
    - 中文别名：按子串匹配
    """

    def __init__(self, assets: list[AssetCfg]) -> None:
        self._patterns: list[tuple[str, list[re.Pattern[str]]]] = [
            (asset.symbol, [self._compile(alias) for alias in (asset.symbol, *asset.aliases)])
            for asset in assets
        ]

    @staticmethod
    def _compile(alias: str) -> re.Pattern[str]:
        escaped = re.escape(alias)
        if alias.isascii():
            flags = 0 if alias.isupper() else re.IGNORECASE
            return re.compile(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", flags)
        return re.compile(escaped)

    def find(self, text: str | None) -> list[str]:
        """返回文本中出现的关注资产代码（按配置顺序、去重）；文本先做 NFKC（全角转半角）"""
        content = unicodedata.normalize("NFKC", text or "")
        return [symbol for symbol, patterns in self._patterns if any(p.search(content) for p in patterns)]
