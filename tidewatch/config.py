"""配置加载

按顺序加载 config.yaml（公开默认值）→ config.local.yaml（私有覆盖，深度合并）→ .env（密钥），
并用 pydantic 校验。校验失败时抛出 ConfigError，错误信息为中文。
"""

from __future__ import annotations

import os
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


class WatchlistCfg(_Strict):
    assets: list[AssetCfg]


class KeywordRule(_Strict):
    id: str
    all: list[str] = []
    any: list[str] = []
    none: list[str] = []

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
    people: list[str]


class ExchangeRulesCfg(_Strict):
    exchange_names: list[str]
    listing_keywords: list[str]


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
