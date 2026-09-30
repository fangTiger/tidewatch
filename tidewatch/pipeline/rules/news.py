"""金十快讯分级规则（系统设计 6.6）

S：important=1 且命中 S 关键词组
A：命中 S 关键词组但非重要；或 important=1 且（涉及关注资产或命中 A 关键词组）；
   或涉及关注资产且出现特朗普/马斯克等人物
B：important=1 的其他快讯；或涉及关注资产
C：其他
"""

from __future__ import annotations

from tidewatch.config import NewsRulesCfg
from tidewatch.models import Event, RuleResult
from tidewatch.pipeline.rules.engine import RuleContext
from tidewatch.pipeline.rules.keywords import matched_ids, text_contains


class NewsRules:
    def __init__(self, cfg: NewsRulesCfg) -> None:
        self._cfg = cfg

    async def evaluate(self, event: Event, ctx: RuleContext) -> RuleResult:
        text = f"{event.title}\n{event.body}"
        important = bool(event.raw.get("important"))
        s_hits = matched_ids(text, self._cfg.s_keywords)
        a_hits = matched_ids(text, self._cfg.a_keywords)
        has_asset = bool(event.assets)
        # 人物匹配与关键词规则语义一致：NFKC 规范化，英文名按独立单词匹配
        has_person = any(text_contains(text, person) for person in self._cfg.people)

        if important and s_hits:
            return RuleResult("S", ["news.important", *s_hits])
        if s_hits:
            return RuleResult("A", s_hits)
        if important and has_asset:
            return RuleResult("A", ["news.important", "news.asset"])
        if important and a_hits:
            return RuleResult("A", ["news.important", *a_hits])
        if has_asset and has_person:
            return RuleResult("A", ["news.asset", "news.person"])
        if important:
            return RuleResult("B", ["news.important"])
        if has_asset:
            return RuleResult("B", ["news.asset"])
        return RuleResult("C", [])
