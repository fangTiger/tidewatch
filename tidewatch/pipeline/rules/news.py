"""金十快讯分级规则（系统设计 6.6）"""

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
        has_noise = any(text_contains(text, marker) for marker in self._cfg.noise_markers)

        if important and s_hits:
            result = RuleResult("S", ["news.important", *s_hits])
        elif s_hits:
            critical = any(rule_id in self._cfg.critical_ids for rule_id in s_hits)
            result = RuleResult("A" if critical else "B", s_hits)
        elif important and has_asset:
            result = RuleResult("A", ["news.important", "news.asset"])
        elif important and a_hits:
            result = RuleResult("A", ["news.important", *a_hits])
        elif has_asset and has_person:
            result = RuleResult("A", ["news.asset", "news.person"])
        elif important:
            result = RuleResult("B", ["news.important"])
        elif has_asset:
            result = RuleResult("B", ["news.asset"])
        else:
            result = RuleResult("C", [])

        if has_noise:
            if result.level in {"S", "A"}:
                result.level = "B"
            result.hits.append("news.noise")
        return result
