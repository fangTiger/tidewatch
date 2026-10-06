"""配置加载

按顺序加载 config.yaml（公开默认值）→ config.local.yaml（私有覆盖，深度合并）→ .env（密钥），
并用 pydantic 校验。校验失败时抛出 ConfigError，错误信息为中文。
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal, Mapping

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


class ConfigError(Exception):
    """配置错误（消息为中文，可直接展示给用户）"""


class _Strict(BaseModel):
    """所有配置模型的基类：拒绝未知配置项，避免拼写错误被静默忽略"""

    model_config = ConfigDict(extra="forbid")


class AssetCfg(_Strict):
    symbol: str
    aliases: list[str] = []

    @field_validator("symbol")
    @classmethod
    def _symbol_not_blank(cls, value: str) -> str:
        # 空代码会让资产识别失去意义，在配置层提前拦截
        if not value.strip():
            raise ValueError("关注资产代码不能为空")
        # 去除首尾空白，避免 " BTC " 与 "BTC" 被当成不同资产
        return value.strip()

    @field_validator("aliases")
    @classmethod
    def _aliases_not_blank(cls, value: list[str]) -> list[str]:
        # 空别名会匹配任意文本，导致所有快讯都被识别为该资产
        if any(not alias.strip() for alias in value):
            raise ValueError("关注资产别名不能为空")
        # 去除首尾空白，否则 " Bitcoin " 永远匹配不到 "Bitcoin rises"
        return [alias.strip() for alias in value]


class WatchlistCfg(_Strict):
    assets: list[AssetCfg]


class KeywordRule(_Strict):
    id: str
    all: list[str] = []
    any: list[str] = []
    none: list[str] = []

    @field_validator("all", "any", "none")
    @classmethod
    def _keywords_not_blank(cls, value: list[str]) -> list[str]:
        # 空关键词会匹配任意文本，导致规则失去意义
        if any(not word.strip() for word in value):
            raise ValueError("关键词不能为空")
        return value

    @model_validator(mode="after")
    def _need_all_or_any(self) -> KeywordRule:
        # 只有 none 的规则无法匹配任何内容，在配置层提前拦截
        if not self.all and not self.any:
            raise ValueError("关键词规则至少需要 all 或 any 之一")
        return self


class CollectorCfg(_Strict):
    enabled: bool = True
    interval_s: float = Field(gt=0)
    proxy: Literal["global", "direct"] = "global"
    stale_after_s: float = Field(gt=0)
    backfill_max_s: float = Field(default=21600, gt=0)
    max_push_age_s: float | None = Field(default=1800, gt=0)
    gap_fill_interval_s: float | None = Field(default=None, gt=0)
    rate_per_s: float = Field(default=2.0, gt=0)


class NewsRulesCfg(_Strict):
    s_keywords: list[KeywordRule]
    a_keywords: list[KeywordRule]
    critical_ids: list[str]
    noise_markers: list[str]
    people: list[str]

    @field_validator("people", "noise_markers")
    @classmethod
    def _text_markers_not_blank(cls, value: list[str], info) -> list[str]:
        # 空字符串会匹配任意文本，导致所有快讯被误升级或封顶
        if any(not item.strip() for item in value):
            message = "人物名不能为空" if info.field_name == "people" else "噪音标记不能为空"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def _critical_ids_exist(self) -> NewsRulesCfg:
        s_ids = {rule.id for rule in self.s_keywords}
        missing = [rule_id for rule_id in self.critical_ids if rule_id not in s_ids]
        if missing:
            raise ValueError(f"critical_ids 中的规则不存在于 s_keywords：{', '.join(missing)}")
        return self


class ExchangeRulesCfg(_Strict):
    exchange_names: list[str]
    listing_keywords: list[str]

    @field_validator("exchange_names")
    @classmethod
    def _names_not_blank(cls, value: list[str]) -> list[str]:
        # 空交易所名会让「含交易所名」的判断恒为真，导致快讯被误并入公告簇
        if any(not name.strip() for name in value):
            raise ValueError("交易所名称不能为空")
        return value

    @field_validator("listing_keywords")
    @classmethod
    def _listing_keywords_not_blank(cls, value: list[str]) -> list[str]:
        # 空关键词会让「含上币/下架关键词」的判断恒为真
        if any(not word.strip() for word in value):
            raise ValueError("上币/下架关键词不能为空")
        return value


class RulesCfg(_Strict):
    news: NewsRulesCfg
    exchange: ExchangeRulesCfg


class PipelineCfg(_Strict):
    cluster_window_s: float = Field(default=600, gt=0)
    title_similarity: float = Field(default=0.5, gt=0, le=1)


class DispatchCfg(_Strict):
    a_window_s: float = Field(default=300, gt=0)
    a_window_max: int = Field(default=10, gt=0)
    digest_interval_s: float = Field(default=60, gt=0)
    retry_immediate: int = Field(default=3, ge=0)
    retry_interval_s: float = Field(default=60, gt=0)
    retry_max_s: float = Field(default=86400, gt=0)
    health_alert_interval_s: float = Field(default=3600, gt=0)


class WebCfg(_Strict):
    host: str = "127.0.0.1"
    port: int = Field(default=8000, gt=0, lt=65536)
    timeline_limit: int = Field(default=200, gt=0)


class AICfg(_Strict):
    enabled: bool = False


class LogCfg(_Strict):
    dir: str = "log"
    retention_days: int = Field(default=14, gt=0)
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


class HyperliquidFrequenciesCfg(_Strict):
    """Hyperliquid 调度与协议频率（秒）"""

    ws_ping_interval_s: float = Field(default=50, gt=0)
    core_state_interval_s: float = Field(default=5, gt=0)
    trade_debounce_s: float = Field(default=3, gt=0)
    trade_max_wait_s: float = Field(default=5, gt=0)
    hot_reconcile_interval_s: float = Field(default=20, gt=0)
    focus_reconcile_interval_s: float = Field(default=180, gt=0)
    hot_reconcile_aging_s: float = Field(default=30, gt=0)
    normal_reconcile_aging_s: float = Field(default=120, gt=0)
    fill_query_min_interval_s: float = Field(default=60, gt=0)
    account_type_cache_s: float = Field(default=21600, gt=0)
    deferred_fill_lookup_timeout_s: float = Field(default=15, gt=0)
    fill_timeout_warning_interval_s: float = Field(default=1800, gt=0)
    leaderboard_interval_s: float = Field(default=43200, gt=0)
    tier_recompute_interval_s: float = Field(default=86400, gt=0)
    profile_interval_s: float = Field(default=86400, gt=0)
    profile_initial_delay_s: float = Field(default=600, ge=0)
    label_refresh_interval_s: float = Field(default=86400, gt=0)
    discovery_window_s: float = Field(default=3, gt=0)
    discovery_cache_s: float = Field(default=3600, gt=0)
    discovery_state_retention_s: float = Field(default=7200, gt=0)
    discovery_cleanup_interval_s: float = Field(default=300, gt=0)
    discovery_summary_interval_s: float = Field(default=300, gt=0)


class HyperliquidTiersCfg(_Strict):
    """钱包分层及热点集合上限"""

    core_limit: int = Field(default=10, gt=0, le=10)
    focus_limit: int = Field(default=200, gt=0)
    candidate_limit: int = Field(default=1000, gt=0)
    hot_limit: int = Field(default=30, gt=0)

    @model_validator(mode="after")
    def _hot_not_above_focus(self) -> HyperliquidTiersCfg:
        if self.hot_limit > self.focus_limit:
            raise ValueError("热点钱包上限不能大于重点钱包上限")
        return self


class HyperliquidEventsCfg(_Strict):
    """巨鲸发现、仓位事件与风险分级门槛"""

    cluster_window_s: float = Field(default=1800, gt=0)
    max_push_age_s: float = Field(default=600, gt=0)
    s_level_notional_usd: float = Field(default=50_000_000, gt=0)
    a_level_notional_usd: float = Field(default=5_000_000, gt=0)
    btc_eth_min_notional_usd: float = Field(default=1_000_000, gt=0)
    top_alt_min_notional_usd: float = Field(default=500_000, gt=0)
    other_min_notional_usd: float = Field(default=250_000, gt=0)
    position_change_ratio: float = Field(default=0.2, gt=0, le=1)
    discovery_trade_notional_usd: float = Field(default=5_000_000, gt=0)
    discovery_account_value_usd: float = Field(default=1_000_000, gt=0)
    hot_position_notional_usd: float = Field(default=10_000_000, gt=0)
    hot_liquidation_distance_ratio: float = Field(default=0.1, gt=0, le=1)
    liquidation_risk_ratio: float = Field(default=0.05, gt=0, le=1)
    liquidation_critical_ratio: float = Field(default=0.02, gt=0, le=1)
    liquidation_hysteresis_ratio: float = Field(default=0.01, gt=0, le=1)
    liquidated_a_level_notional_usd: float = Field(default=5_000_000, gt=0)
    configured_leverage_threshold: float = Field(default=20, gt=0)
    actual_leverage_threshold: float = Field(default=10, gt=0)

    @model_validator(mode="after")
    def _threshold_order(self) -> HyperliquidEventsCfg:
        if self.s_level_notional_usd <= self.a_level_notional_usd:
            raise ValueError("S 级名义价值门槛必须大于 A 级门槛")
        if self.liquidation_critical_ratio >= self.liquidation_risk_ratio:
            raise ValueError("强平风险 S 级距离必须小于 A 级距离")
        if self.liquidation_hysteresis_ratio >= self.liquidation_risk_ratio:
            raise ValueError("强平风险滞回必须小于 A 级距离")
        return self


class HyperliquidHealthSourceCfg(_Strict):
    stale_after_s: float = Field(gt=0)


class HyperliquidHealthCfg(_Strict):
    """Hyperliquid 五个相互独立的健康子源阈值"""

    hl_rest: HyperliquidHealthSourceCfg
    hl_trades_ws: HyperliquidHealthSourceCfg
    hl_core_ws: HyperliquidHealthSourceCfg
    hl_leaderboard: HyperliquidHealthSourceCfg
    hl_profile: HyperliquidHealthSourceCfg


class HyperliquidCfg(_Strict):
    """Hyperliquid 客户端、调度、规则与健康配置"""

    enabled: bool = True
    proxy: Literal["global", "direct"] = "global"
    rest_url: str = "https://api.hyperliquid.xyz/info"
    ws_url: str = "wss://api.hyperliquid.xyz/ws"
    leaderboard_url: str = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
    aliases_url: str = "https://api.hypurrscan.io/globalAliases"
    monitor_budget_per_min: float = Field(default=600, gt=0)
    batch_budget_per_min: float = Field(default=200, gt=0)
    discovery_query_limit_per_min: int = Field(default=3, gt=0)
    reconcile_concurrency: int = Field(default=4, gt=0)
    profile_retry_limit: int = Field(default=3, gt=0)
    profile_candidate_shards: int = Field(default=2, gt=0)
    profile_failure_ratio_threshold: float = Field(default=0.1, gt=0, le=1)
    profile_wallet_timeout_s: float = Field(default=600, gt=0)
    rest_burst_window_s: float = Field(default=10, gt=0)
    aimd_min_budget_per_min: float = Field(default=120, gt=0)
    aimd_recovery_interval_s: float = Field(default=60, gt=0)
    frequencies: HyperliquidFrequenciesCfg = HyperliquidFrequenciesCfg()
    tiers: HyperliquidTiersCfg = HyperliquidTiersCfg()
    events: HyperliquidEventsCfg = HyperliquidEventsCfg()
    hip3_dexes: list[str] = []
    top_altcoins: list[str] = []
    ticker_symbols: list[str] = []
    core_wallets: list[str] = []
    protocol_excluded_addresses: list[str] = []
    health: HyperliquidHealthCfg

    @field_validator("rest_url", "leaderboard_url", "aliases_url")
    @classmethod
    def _rest_url_scheme(cls, value: str) -> str:
        if value != value.strip() or not value.startswith(("http://", "https://")):
            raise ValueError("REST 端点必须是 http:// 或 https:// 地址")
        return value

    @field_validator("ws_url")
    @classmethod
    def _ws_url_scheme(cls, value: str) -> str:
        if value != value.strip() or not value.startswith(("ws://", "wss://")):
            raise ValueError("WS 端点必须是 ws:// 或 wss:// 地址")
        return value

    @field_validator("hip3_dexes", "top_altcoins", "ticker_symbols")
    @classmethod
    def _symbols_not_blank_or_duplicate(cls, value: list[str], info) -> list[str]:
        labels = {
            "hip3_dexes": "HIP-3 dex",
            "top_altcoins": "头部山寨币",
            "ticker_symbols": "行情币种",
        }
        label = labels[info.field_name]
        normalized = [symbol.strip() for symbol in value]
        if any(not symbol for symbol in normalized):
            raise ValueError(f"{label}不能为空")
        if len(set(normalized)) != len(normalized):
            raise ValueError(f"{label}不能重复")
        return normalized

    @field_validator("core_wallets", "protocol_excluded_addresses", mode="before")
    @classmethod
    def _wallet_addresses_valid(cls, value: Any, info) -> list[str]:
        if not isinstance(value, list):
            return value
        label = "核心钱包地址" if info.field_name == "core_wallets" else "协议排除地址"
        if info.field_name == "core_wallets" and len(value) > 10:
            raise ValueError("核心钱包最多配置 10 个")
        normalized: list[str] = []
        for address in value:
            # YAML 会把未加引号的 0x... 解析成整数；按 160 位地址还原，避免示例配置踩坑。
            if isinstance(address, int) and not isinstance(address, bool) and 0 <= address < 2**160:
                normalized.append(f"0x{address:040x}")
                continue
            if not isinstance(address, str) or not re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
                raise ValueError(f"{label}必须是 0x 开头的 40 位十六进制地址")
            normalized.append(address.lower())
        if len(set(normalized)) != len(normalized):
            raise ValueError(f"{label}不能重复")
        return normalized

    @model_validator(mode="after")
    def _low_frequency_health_exceeds_period(self) -> HyperliquidCfg:
        checks = (
            (
                "hl_leaderboard",
                self.health.hl_leaderboard.stale_after_s,
                self.frequencies.leaderboard_interval_s,
            ),
            ("hl_profile", self.health.hl_profile.stale_after_s, self.frequencies.profile_interval_s),
        )
        for source, stale_after_s, interval_s in checks:
            if stale_after_s <= interval_s:
                raise ValueError(f"{source}.stale_after_s 必须大于正常周期")
        return self


class Secrets(_Strict):
    """密钥集合：所有字段都不出现在 repr 中，避免日志或异常回溯泄露"""

    database_url: str = Field(repr=False)
    feishu_webhook: str | None = Field(default=None, repr=False)
    feishu_secret: str | None = Field(default=None, repr=False)
    binance_api_key: str | None = Field(default=None, repr=False)
    binance_api_secret: str | None = Field(default=None, repr=False)

    @field_validator("database_url")
    @classmethod
    def _asyncpg_only(cls, value: str) -> str:
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("数据库连接必须以 postgresql+asyncpg:// 开头")
        return value


class Settings(_Strict):
    proxy_url: str | None = None
    watchlist: WatchlistCfg
    collectors: dict[str, CollectorCfg]
    rules: RulesCfg
    pipeline: PipelineCfg = PipelineCfg()
    dispatch: DispatchCfg = DispatchCfg()
    web: WebCfg = WebCfg()
    hyperliquid: HyperliquidCfg
    ai: AICfg = AICfg()
    log: LogCfg = LogCfg()
    secrets: Secrets = Field(repr=False)

    def collector(self, name: str) -> CollectorCfg:
        """按名称取采集器配置，不存在时抛出 ConfigError"""
        try:
            return self.collectors[name]
        except KeyError:
            raise ConfigError(f"缺少采集器配置：collectors.{name}") from None

    @property
    def watch_symbols(self) -> set[str]:
        return {asset.symbol for asset in self.watchlist.assets}


# .env / 环境变量 → Secrets 字段
_ENV_KEYS = {
    "database_url": "TIDEWATCH_DATABASE_URL",
    "feishu_webhook": "FEISHU_WEBHOOK",
    "feishu_secret": "FEISHU_SECRET",
    "binance_api_key": "BINANCE_API_KEY",
    "binance_api_secret": "BINANCE_API_SECRET",
}

# 对象列表按这些标识键合并（例如关注资产按 symbol、关键词规则按 id）
_MERGE_KEYS = ("symbol", "id")


def deep_merge(base: Any, override: Any) -> Any:
    """深度合并

    - 字典逐键合并
    - 以 symbol/id 标识的对象列表按标识合并；override 中带 `remove: true` 的项会删除对应默认项
    - override 为空列表时直接替换（用于清空）
    - 其他值由 override 覆盖
    """
    if isinstance(base, dict) and isinstance(override, dict):
        merged = dict(base)
        for key, value in override.items():
            merged[key] = deep_merge(base[key], value) if key in base else value
        return merged
    if isinstance(base, list) and isinstance(override, list) and override:
        key = _list_merge_key(base + override)
        if key is not None:
            merged_items: dict[Any, Any] = {item[key]: item for item in base}
            for item in override:
                if item.get("remove"):
                    merged_items.pop(item[key], None)
                    continue
                existing = merged_items.get(item[key])
                merged_items[item[key]] = deep_merge(existing, item) if existing else item
            return list(merged_items.values())
    return override


def _list_merge_key(items: list[Any]) -> str | None:
    for key in _MERGE_KEYS:
        if items and all(isinstance(item, dict) and key in item for item in items):
            return key
    return None


def load_settings(base_dir: Path | str, environ: Mapping[str, str] | None = None) -> Settings:
    """加载并校验配置

    Args:
        base_dir: 配置文件所在目录；其中的 config.yaml、config.local.yaml、.env 都从这里读取
        environ: 额外环境变量，优先级高于 .env；为 None 时使用 os.environ
    """
    base = Path(base_dir)
    default_path = base / "config.yaml"
    if not default_path.exists():
        raise ConfigError(f"找不到默认配置文件：{default_path}")
    data = _read_yaml(default_path)
    local_path = base / "config.local.yaml"
    if local_path.exists():
        data = deep_merge(data, _read_yaml(local_path))

    env = {**dotenv_values(base / ".env"), **(os.environ if environ is None else environ)}
    data["secrets"] = {field: env[var] for field, var in _ENV_KEYS.items() if env.get(var)}
    try:
        return Settings.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(_format_errors(exc)) from None


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"配置文件 {path.name} 不是合法的 YAML：{exc}") from None
    if content is None:
        return {}
    if not isinstance(content, dict):
        raise ConfigError(f"配置文件 {path.name} 的顶层必须是键值映射")
    if "secrets" in content:
        # 密钥只能来自 .env / 环境变量，避免误提交到公开仓库
        raise ConfigError("密钥不能写在配置文件中，请写到 .env")
    return content


def _format_errors(exc: ValidationError) -> str:
    lines = ["配置校验失败："]
    for err in exc.errors():
        loc = ".".join(str(part) for part in err["loc"])
        if loc == "secrets.database_url" and err["type"] == "missing":
            lines.append("  - 缺少数据库连接：请在 .env 中设置 TIDEWATCH_DATABASE_URL")
            continue
        lines.append(f"  - 配置项 {loc} 无效：{_translate(err)}")
    return "\n".join(lines)


_SIMPLE_MESSAGES = {
    "missing": "缺少必填项",
    "extra_forbidden": "未知配置项（请检查拼写）",
    "float_parsing": "必须是数字",
    "int_parsing": "必须是整数",
    "bool_parsing": "必须是 true 或 false",
    "string_type": "必须是字符串",
    "list_type": "必须是列表",
    "dict_type": "必须是键值映射",
    "model_type": "必须是键值映射",
}


def _num(value: Any) -> str:
    return f"{value:g}" if isinstance(value, (int, float)) else str(value)


def _translate(err: Mapping[str, Any]) -> str:
    kind = err["type"]
    ctx = err.get("ctx") or {}
    if kind in _SIMPLE_MESSAGES:
        return _SIMPLE_MESSAGES[kind]
    if kind == "greater_than":
        return f"必须大于 {_num(ctx.get('gt'))}"
    if kind == "greater_than_equal":
        return f"必须大于或等于 {_num(ctx.get('ge'))}"
    if kind == "less_than":
        return f"必须小于 {_num(ctx.get('lt'))}"
    if kind == "less_than_equal":
        return f"必须小于或等于 {_num(ctx.get('le'))}"
    if kind == "literal_error":
        return f"取值必须为 {str(ctx.get('expected')).replace(' or ', ' 或 ')}"
    if kind == "value_error":
        return str(ctx.get("error", err["msg"]))
    return f"{err['msg']}（输入：{err.get('input')!r}）"
