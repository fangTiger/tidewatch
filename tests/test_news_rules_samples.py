import asyncio
from datetime import datetime, timezone

import pytest

from tidewatch.models import Event
from tidewatch.pipeline.rules.engine import RuleContext
from tidewatch.pipeline.rules.news import NewsRules

NOW = datetime(2026, 10, 1, 2, 0, tzinfo=timezone.utc)
CTX = RuleContext(conn=None, now=NOW)


def _flash(text: str, *, important: int, assets: list[str] | None = None) -> Event:
    return Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=NOW,
        title=text,
        body=text,
        dedupe_key=f"jin10:{text}",
        assets=assets or [],
        raw={"important": important},
    )


@pytest.mark.parametrize(
    ("important", "text", "expected", "assets"),
    [
        (
            0,
            "巴基斯坦财政部：预计9月消费者价格指数（CPI）通胀率为10%-11%，短期内仍将维持在较高水平。",
            "C",
            [],
        ),
        (1, "2027年FOMC票委、里奇蒙联储主席巴尔金将于十分钟后在一场活动上致欢迎词。", "B", []),
        (
            1,
            "【期货盯盘神器专属文章】市场深谈：PCE不及预期之后，美联储还会加几次息？摩根士丹利的Khanduja认为，市场对到明年年底加息3—4次的定价过于激进。点击阅读。",
            "B",
            [],
        ),
        (
            1,
            "今日重点关注的财经数据与事件：2026月10月1日 周四 ① 14:00 英国9月Nationwide房价指数月率 ② 14:30 瑞士9月CPI月率",
            "B",
            [],
        ),
        (
            0,
            "美国8月核心PCE通胀数据意外低于预期，但美联储官员密集释放鹰派信号，重申需压制通胀；欢迎前往“数据库-嘉盛市场晴雨表”查看并订阅",
            "B",
            [],
        ),
        (1, "日本央行9月意见摘要：政策重点转向防止通胀超调，委员讨论加快加息。点击查看…", "B", []),
        (1, "纳指期货日内上涨1%，标普500指数期货上涨0.5%，道指期货上涨0.3%。", "A", ["NDX"]),
        (1, "市场对主要央行的紧缩定价已有所过度，最终加息路径或比市场预期更加平缓", "A", []),
        (1, "美国9月季调后非农就业人口 8.2万，预期12万，前值2.2万", "S", []),
        (1, "美国9月未季调CPI年率 3.1%，预期3.0%，前值2.9%", "S", []),
        (1, "美国8月核心PCE物价指数年率 2.9%，预期3.0%", "S", []),
        (1, "美联储宣布降息25个基点，将联邦基金利率目标区间下调至3.75%-4.00%，符合市场预期", "S", []),
        (1, "美联储主席在新闻发布会上表示，通胀仍然过高", "S", []),
        (1, "欧洲央行利率决议：维持三大关键利率不变", "S", []),
        (1, "美国9月CPI年率将于十分钟后公布", "B", []),
        (0, "某加密货币交易所被盗约1亿美元", "A", []),
        (0, "稳定币USDX出现脱锚", "A", []),
        (0, "美国9月CPI年率 3.1%", "B", []),
    ],
)
async def test_real_jin10_samples(settings, important, text, expected, assets):
    result = await asyncio.wait_for(
        NewsRules(settings.rules.news).evaluate(
            _flash(text, important=important, assets=assets), CTX
        ),
        timeout=1,
    )
    assert result.level == expected


async def test_rate_expectation_is_not_s(settings):
    text = "美联储降息预期升温，交易员押注年内再降息两次"
    result = await asyncio.wait_for(
        NewsRules(settings.rules.news).evaluate(_flash(text, important=1), CTX), timeout=1
    )
    assert result.level != "S"
