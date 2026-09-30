from datetime import datetime, timezone

from tidewatch.config import KeywordRule, NewsRulesCfg
from tidewatch.models import Event, RuleResult
from tidewatch.pipeline.rules.engine import RuleContext, RuleEngine
from tidewatch.pipeline.rules.keywords import match_rule, text_contains
from tidewatch.pipeline.rules.news import NewsRules

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
CTX = RuleContext(conn=None, now=NOW)


def _flash(title: str, *, important: int = 0, assets: list[str] | None = None, body: str = "") -> Event:
    return Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=NOW,
        title=title,
        body=body,
        dedupe_key=f"jin10:{title}",
        assets=assets or [],
        raw={"important": important},
    )


def test_keyword_rule_combinations():
    tariff = KeywordRule(id="t", all=["关税"], any=["中国", "全面"], none=["传闻"])
    assert match_rule("美国宣布对中国加征关税", tariff)
    assert not match_rule("美国宣布对欧盟加征关税", tariff)  # any 未命中
    assert not match_rule("传闻美国将对中国加征关税", tariff)  # 命中排除词
    assert match_rule("sec 主席讲话", KeywordRule(id="s", any=["SEC"]))  # 英文不区分大小写
    assert not match_rule("consecutive gains", KeywordRule(id="s", any=["SEC"]))  # 英文按独立单词匹配
    assert match_rule("美国ＣＰＩ同比上涨", KeywordRule(id="c", any=["CPI"]))  # 全角也能匹配
    assert text_contains("Binance Will List XYZ", "list") and not text_contains("Binance blacklist", "list")
    # all 与 any 都为空时不命中（配置层已拦截此类规则，这里用 model_construct 绕过校验验证防御行为）
    assert not match_rule("任意文本", KeywordRule.model_construct(id="empty"))


def test_blank_keyword_never_matches():
    # 规范化后为空的关键词不应匹配任何文本（配置层已拦截，这里验证防御行为）
    assert not text_contains("任意文本", "")
    assert not text_contains("任意文本", "   ")
    assert not match_rule("任意文本", KeywordRule.model_construct(id="b", all=[], any=[""], none=[]))


async def _level(rules: NewsRules, event: Event) -> RuleResult:
    return await rules.evaluate(event, CTX)


async def test_news_levels(settings):
    rules = NewsRules(settings.rules.news)
    r = await _level(rules, _flash("美国9月非农就业人口增加8.2万人，预期12万人", important=1))
    assert r.level == "S" and "s.nfp" in r.hits
    assert (await _level(rules, _flash("美国9月非农就业人口增加8.2万人"))).level == "A"
    assert (await _level(rules, _flash("比特币跌破11万美元", important=1, assets=["BTC"]))).level == "A"
    assert (await _level(rules, _flash("美联储理事沃勒：支持12月降息", important=1))).level == "A"
    assert (await _level(rules, _flash("特朗普：比特币是未来", assets=["BTC"]))).level == "A"
    assert (await _level(rules, _flash("港股低开高走，恒指翻红", important=1))).level == "B"
    assert (await _level(rules, _flash("以太坊链上活跃度回升", assets=["ETH"]))).level == "B"
    assert (await _level(rules, _flash("美国宣布对欧盟汽车加征关税"))).level == "C"
    assert (await _level(rules, _flash("印度Axis银行将提供Apple Pay服务"))).level == "C"
    tariff = await _level(rules, _flash("美国宣布对中国加征关税", important=1))
    assert tariff.level == "S" and "s.tariff" in tariff.hits
    # 关键词只出现在正文中也能命中
    assert (await _level(rules, _flash("美国劳工部公布最新数据", important=1, body="9月非农就业人口增加8.2万"))).level == "S"
    # 前瞻类文章不算 S
    assert (await _level(rules, _flash("本周非农前瞻：就业或继续放缓", important=1))).level == "B"


async def test_engine_dispatch_by_category(settings):
    engine = RuleEngine({"news": NewsRules(settings.rules.news)})
    assert (await engine.evaluate(_flash("美国9月非农就业人口增加8.2万人"), CTX)).level == "A"
    other = _flash("未知类别")
    other.category = "unknown"
    result = await engine.evaluate(other, CTX)
    assert result.level == "C" and result.hits == ["no_rules"]


async def test_person_matching_reuses_keyword_semantics():
    cfg = NewsRulesCfg(s_keywords=[], a_keywords=[], people=["Trump"])
    rules = NewsRules(cfg)
    # 英文人物名按独立单词匹配，且不区分大小写
    assert (await _level(rules, _flash("Trump says bitcoin", assets=["BTC"]))).level == "A"
    assert (await _level(rules, _flash("TRUMP：比特币", assets=["BTC"]))).level == "A"
    assert (await _level(rules, _flash("trumpet 演奏", assets=["BTC"]))).level == "B"
    # 全角字符经 NFKC 规范化后也能匹配
    assert (await _level(rules, _flash("Ｔｒｕｍｐ 发言", assets=["BTC"]))).level == "A"
