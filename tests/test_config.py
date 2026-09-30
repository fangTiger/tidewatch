from pathlib import Path

import pytest

from pydantic import ValidationError

from tidewatch.config import (
    AssetCfg,
    ConfigError,
    ExchangeRulesCfg,
    KeywordRule,
    NewsRulesCfg,
    deep_merge,
    load_settings,
)
from tidewatch.pipeline.normalize import AssetMatcher

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV = {"TIDEWATCH_DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/tidewatch"}


def _copy_default(tmp_path: Path) -> Path:
    (tmp_path / "config.yaml").write_text(
        (REPO_ROOT / "config.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    return tmp_path


def test_repo_default_config_is_valid(tmp_path):
    settings = load_settings(_copy_default(tmp_path), environ=ENV)
    assert settings.collector("jin10").proxy == "direct"
    assert settings.ai.enabled is False
    assert settings.secrets.database_url.startswith("postgresql+asyncpg://")


def test_local_config_adds_asset_and_env_file(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        "proxy_url: http://127.0.0.1:10808\n"
        "watchlist:\n  assets:\n    - {symbol: DOGE, aliases: [狗狗币]}\n",
        encoding="utf-8",
    )
    (base / ".env").write_text("FEISHU_WEBHOOK=https://example.com/hook\n", encoding="utf-8")
    settings = load_settings(base, environ=ENV)
    symbols = [a.symbol for a in settings.watchlist.assets]
    assert "BTC" in symbols and "DOGE" in symbols
    assert settings.proxy_url == "http://127.0.0.1:10808"
    assert settings.secrets.feishu_webhook == "https://example.com/hook"


def test_local_config_overrides_existing_asset(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        "watchlist:\n  assets:\n    - {symbol: BTC, aliases: [大饼]}\n", encoding="utf-8"
    )
    settings = load_settings(base, environ=ENV)
    btc = [a for a in settings.watchlist.assets if a.symbol == "BTC"]
    assert len(btc) == 1 and btc[0].aliases == ["大饼"]


def test_invalid_interval_reports_chinese(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        "collectors:\n  jin10:\n    interval_s: -1\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    message = str(exc.value)
    assert "collectors.jin10.interval_s" in message
    assert "必须大于 0" in message


def test_missing_database_url(tmp_path):
    with pytest.raises(ConfigError) as exc:
        load_settings(_copy_default(tmp_path), environ={})
    assert "TIDEWATCH_DATABASE_URL" in str(exc.value)


def test_unknown_collector_name(tmp_path):
    settings = load_settings(_copy_default(tmp_path), environ=ENV)
    with pytest.raises(ConfigError):
        settings.collector("not_exist")


def test_deep_merge_rules():
    base = {"a": {"b": 1, "c": [1, 2]}, "items": [{"id": "x", "v": 1}, {"id": "z", "v": 9}]}
    override = {"a": {"c": [3]}, "items": [{"id": "x", "v": 2}, {"id": "y", "v": 3}, {"id": "z", "remove": True}]}
    merged = deep_merge(base, override)
    assert merged["a"] == {"b": 1, "c": [3]}
    assert merged["items"] == [{"id": "x", "v": 2}, {"id": "y", "v": 3}]
    # 空列表直接替换
    assert deep_merge({"items": [{"id": "x"}]}, {"items": []}) == {"items": []}


def test_local_config_removes_asset(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        "watchlist:\n  assets:\n    - {symbol: TSLA, remove: true}\n", encoding="utf-8"
    )
    settings = load_settings(base, environ=ENV)
    assert "TSLA" not in settings.watch_symbols and "BTC" in settings.watch_symbols


def test_unknown_key_rejected(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text("collectors:\n  jin10:\n    interval: 5\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    assert "collectors.jin10.interval 无效：未知配置项" in str(exc.value)


def test_env_var_overrides_dotenv_file(tmp_path):
    base = _copy_default(tmp_path)
    (base / ".env").write_text(
        "TIDEWATCH_DATABASE_URL=postgresql+asyncpg://file:file@localhost:5432/from_file\n"
        "FEISHU_WEBHOOK=https://example.com/from_file\n",
        encoding="utf-8",
    )
    environ = {**ENV, "FEISHU_WEBHOOK": "https://example.com/from_env"}
    settings = load_settings(base, environ=environ)
    # 环境变量优先于 .env；环境变量未设置的项仍取自 .env
    assert settings.secrets.database_url == ENV["TIDEWATCH_DATABASE_URL"]
    assert settings.secrets.feishu_webhook == "https://example.com/from_env"


def test_database_url_must_use_asyncpg(tmp_path):
    environ = {"TIDEWATCH_DATABASE_URL": "postgresql://u:leakme@localhost:5432/tidewatch"}
    with pytest.raises(ConfigError) as exc:
        load_settings(_copy_default(tmp_path), environ=environ)
    message = str(exc.value)
    assert "数据库连接必须以 postgresql+asyncpg:// 开头" in message
    # 报错信息不能回显密码
    assert "leakme" not in message


def test_repr_and_str_do_not_leak_secrets(tmp_path):
    environ = {
        "TIDEWATCH_DATABASE_URL": "postgresql+asyncpg://u:s3cretpw@localhost:5432/tidewatch",
        "FEISHU_WEBHOOK": "https://example.com/hook/tokentokentoken",
        "FEISHU_SECRET": "signsecretvalue",
    }
    settings = load_settings(_copy_default(tmp_path), environ=environ)
    for text in (repr(settings), str(settings), repr(settings.secrets), str(settings.secrets)):
        assert "s3cretpw" not in text
        assert "tokentokentoken" not in text
        assert "signsecretvalue" not in text
    # 仍然能正常取到明文供程序使用
    assert settings.secrets.database_url == environ["TIDEWATCH_DATABASE_URL"]


def test_empty_section_reports_chinese(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text("watchlist:\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    message = str(exc.value)
    assert "watchlist" in message and "必须是键值映射" in message
    assert "Input should be" not in message


def test_literal_error_uses_chinese_or(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text("collectors:\n  jin10:\n    proxy: foo\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    message = str(exc.value)
    assert "collectors.jin10.proxy" in message
    assert "'global' 或 'direct'" in message and " or " not in message


def test_keyword_rule_requires_all_or_any(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        "rules:\n  news:\n    s_keywords:\n      - {id: s.empty, none: [前瞻]}\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    message = str(exc.value)
    assert "rules.news.s_keywords" in message
    assert "关键词规则至少需要 all 或 any 之一" in message


def test_asset_alias_must_not_be_blank(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        'watchlist:\n  assets:\n    - {symbol: BTC, aliases: [""]}\n', encoding="utf-8"
    )
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    message = str(exc.value)
    assert "watchlist.assets" in message and "aliases" in message
    assert "关注资产别名不能为空" in message


def test_asset_symbol_must_not_be_blank(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        'watchlist:\n  assets:\n    - {symbol: "  ", aliases: [狗狗币]}\n', encoding="utf-8"
    )
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    message = str(exc.value)
    assert "watchlist.assets" in message and "symbol" in message
    assert "关注资产代码不能为空" in message


@pytest.mark.parametrize("field", ["all", "any", "none"])
def test_keyword_rule_items_must_not_be_blank(field):
    kwargs = {"id": "r", "any": ["关税"], field: ["关税", "  "]}
    with pytest.raises(ValidationError) as exc:
        KeywordRule(**kwargs)
    assert "关键词不能为空" in str(exc.value)


def test_keyword_rule_blank_reported_in_chinese(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        'rules:\n  news:\n    s_keywords:\n      - {id: s.blank, any: [""]}\n', encoding="utf-8"
    )
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    message = str(exc.value)
    assert "rules.news.s_keywords" in message and "关键词不能为空" in message


def test_news_people_must_not_be_blank(tmp_path):
    with pytest.raises(ValidationError) as exc:
        NewsRulesCfg(s_keywords=[], a_keywords=[], people=["特朗普", " "])
    assert "人物名不能为空" in str(exc.value)
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text('rules:\n  news:\n    people: ["马斯克", ""]\n', encoding="utf-8")
    with pytest.raises(ConfigError) as config_exc:
        load_settings(base, environ=ENV)
    message = str(config_exc.value)
    assert "rules.news.people" in message and "人物名不能为空" in message


def test_asset_symbol_is_stripped():
    assert AssetCfg(symbol=" BTC ").symbol == "BTC"


def test_asset_aliases_are_stripped():
    # 别名首尾空白必须去掉，否则 " Bitcoin " 永远匹配不到 "Bitcoin rises"
    cfg = AssetCfg(symbol="BTC", aliases=[" Bitcoin ", "\t比特币\n"])
    assert cfg.aliases == ["Bitcoin", "比特币"]
    assert AssetMatcher([cfg]).find("Bitcoin rises") == ["BTC"]


_EXCHANGE_BLANK_CASES = [
    ("exchange_names", "交易所名称不能为空"),
    ("listing_keywords", "上币/下架关键词不能为空"),
]


@pytest.mark.parametrize("field, expected", _EXCHANGE_BLANK_CASES)
def test_exchange_rule_items_must_not_be_blank(field, expected):
    kwargs = {"exchange_names": ["币安"], "listing_keywords": ["上线"]}
    kwargs[field] = ["币安", "  "]
    with pytest.raises(ValidationError) as exc:
        ExchangeRulesCfg(**kwargs)
    assert expected in str(exc.value)


@pytest.mark.parametrize("field, expected", _EXCHANGE_BLANK_CASES)
def test_exchange_rule_blank_reported_in_chinese(tmp_path, field, expected):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(f'rules:\n  exchange:\n    {field}: ["", 币安]\n', encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    message = str(exc.value)
    assert f"rules.exchange.{field}" in message and expected in message


@pytest.mark.parametrize("filename", ["config.yaml", "config.local.yaml"])
def test_secrets_key_in_config_file_rejected(tmp_path, filename):
    base = _copy_default(tmp_path)
    path = base / filename
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    path.write_text(
        existing + "\nsecrets:\n  database_url: postgresql+asyncpg://u:p@localhost/db\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)
    assert str(exc.value) == "密钥不能写在配置文件中，请写到 .env"
