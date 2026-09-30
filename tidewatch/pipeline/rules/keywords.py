"""关键词组合匹配：all（全部包含）+ any（任一包含）+ none（排除）

文本与关键词都先做 NFKC（全角转半角）并转小写；纯英文/数字关键词要求前后不紧挨字母或数字
（避免 SEC 命中 consecutive），中文关键词按子串匹配。
"""

from __future__ import annotations

import re
import unicodedata

from tidewatch.config import KeywordRule


def _normalize(text: str | None) -> str:
    return unicodedata.normalize("NFKC", text or "").lower()


def _contains(content: str, word: str) -> bool:
    """content 须已规范化"""
    needle = _normalize(word)
    if needle.isascii():
        return re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", content) is not None
    return needle in content


def text_contains(text: str | None, word: str) -> bool:
    """文本是否包含关键词（规范化后比较；纯英文/数字关键词按独立单词匹配）"""
    return _contains(_normalize(text), word)


def match_rule(text: str | None, rule: KeywordRule) -> bool:
    """判断文本是否命中规则；all 与 any 都为空的规则视为不命中"""
    content = _normalize(text)
    if not (rule.all or rule.any):
        return False
    if not all(_contains(content, word) for word in rule.all):
        return False
    if rule.any and not any(_contains(content, word) for word in rule.any):
        return False
    return not any(_contains(content, word) for word in rule.none)


def matched_ids(text: str | None, rules: list[KeywordRule]) -> list[str]:
    """返回命中的规则 ID 列表（按配置顺序）"""
    return [rule.id for rule in rules if match_rule(text, rule)]
