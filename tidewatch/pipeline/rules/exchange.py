"""交易所公告分级规则（系统设计 6.6）

| 公告类型 | 币安 | OKX |
| 现货上币 | S | A |
| 合约上线 | A | B |
| 下架 | 关注资产 A，其他 B | 同左 |
| 合约下线 | B | B |
| 暂停充提 | 关注资产 A，其他 C | 同左 |
| 其他 | C | C |
"""

from __future__ import annotations

from tidewatch.models import Event, RuleResult
from tidewatch.pipeline.rules.engine import RuleContext

_LISTING_LEVELS = {
    ("binance", "spot_listing"): "S",
    ("okx", "spot_listing"): "A",
    ("binance", "perp_listing"): "A",
    ("okx", "perp_listing"): "B",
}


class ExchangeRules:
    def __init__(self, watch_symbols: set[str]) -> None:
        self._watch = set(watch_symbols)

    async def evaluate(self, event: Event, ctx: RuleContext) -> RuleResult:
        # 采集器约定在 raw.exchange 写入交易所名（binance / okx）；缺失时按来源名推断
        # 取值后去空白并转小写；为空（含纯空白）时同样按来源名推断
        exchange = str(event.raw.get("exchange") or "").strip().lower() or event.source.removesuffix("_ann")
        kind = event.event_type
        hits = [f"exchange.{exchange}.{kind}"]
        watched = bool(self._watch & set(event.assets))
        if watched:
            hits.append("exchange.watched_asset")

        if (exchange, kind) in _LISTING_LEVELS:
            return RuleResult(_LISTING_LEVELS[(exchange, kind)], hits)
        if kind == "delisting":
            return RuleResult("A" if watched else "B", hits)
        if kind == "perp_delisting":
            return RuleResult("B", hits)
        if kind == "deposit_withdraw_suspend":
            return RuleResult("A" if watched else "C", hits)
        return RuleResult("C", hits)
