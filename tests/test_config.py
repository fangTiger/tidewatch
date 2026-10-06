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
        NewsRulesCfg(
            s_keywords=[],
            a_keywords=[],
            critical_ids=[],
            noise_markers=[],
            people=["特朗普", " "],
        )
    assert "人物名不能为空" in str(exc.value)
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text('rules:\n  news:\n    people: ["马斯克", ""]\n', encoding="utf-8")
    with pytest.raises(ConfigError) as config_exc:
        load_settings(base, environ=ENV)
    message = str(config_exc.value)
    assert "rules.news.people" in message and "人物名不能为空" in message


def test_news_noise_markers_must_not_be_blank(tmp_path):
    with pytest.raises(ValidationError) as exc:
        NewsRulesCfg(
            s_keywords=[],
            a_keywords=[],
            critical_ids=[],
            noise_markers=["点击阅读", " "],
            people=[],
        )
    assert "噪音标记不能为空" in str(exc.value)
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        'rules:\n  news:\n    noise_markers: ["", 点击阅读]\n', encoding="utf-8"
    )
    with pytest.raises(ConfigError) as config_exc:
        load_settings(base, environ=ENV)
    message = str(config_exc.value)
    assert "rules.news.noise_markers" in message and "噪音标记不能为空" in message


def test_news_critical_ids_must_reference_s_keyword_rule(tmp_path):
    with pytest.raises(ValidationError) as exc:
        NewsRulesCfg(
            s_keywords=[KeywordRule(id="s.known", any=["已知"])],
            a_keywords=[],
            critical_ids=["s.missing"],
            noise_markers=[],
            people=[],
        )
    assert "critical_ids 中的规则不存在于 s_keywords：s.missing" in str(exc.value)
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        "rules:\n  news:\n    critical_ids: [s.missing]\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError) as config_exc:
        load_settings(base, environ=ENV)
    message = str(config_exc.value)
    assert "rules.news" in message
    assert "critical_ids 中的规则不存在于 s_keywords：s.missing" in message


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


def test_hyperliquid_default_config_is_valid_and_complete(tmp_path):
    from tidewatch.hyperliquid.leaderboard import HLP_ADDRESSES

    settings = load_settings(_copy_default(tmp_path), environ=ENV)
    cfg = settings.hyperliquid

    assert cfg.enabled is True
    assert cfg.proxy == "global"
    assert cfg.rest_url == "https://api.hyperliquid.xyz/info"
    assert cfg.ws_url == "wss://api.hyperliquid.xyz/ws"
    assert cfg.monitor_budget_per_min == 600
    assert cfg.batch_budget_per_min == 200
    assert cfg.discovery_query_limit_per_min == 3
    assert cfg.reconcile_concurrency == 4
    assert cfg.rest_burst_window_s == 10
    assert cfg.aimd_min_budget_per_min == 120
    assert cfg.aimd_recovery_interval_s == 60
    assert cfg.frequencies.ws_ping_interval_s == 50
    assert cfg.frequencies.trade_max_wait_s == 5
    assert cfg.frequencies.hot_reconcile_interval_s == 20
    assert cfg.frequencies.focus_reconcile_interval_s == 180
    assert cfg.frequencies.hot_reconcile_aging_s == 30
    assert cfg.frequencies.normal_reconcile_aging_s == 120
    assert cfg.frequencies.account_type_cache_s == 86400
    assert cfg.fill_retention_days == 90
    assert cfg.frequencies.deferred_fill_lookup_timeout_s == 15
    assert cfg.frequencies.fill_timeout_warning_interval_s == 1800
    assert cfg.frequencies.discovery_summary_interval_s == 300
    assert cfg.frequencies.leaderboard_interval_s == 12 * 60 * 60
    assert cfg.tiers.core_limit == 10
    assert cfg.tiers.focus_limit == 200
    assert cfg.events.s_level_notional_usd == 50_000_000
    assert cfg.events.a_level_notional_usd == 5_000_000
    assert cfg.events.btc_eth_min_notional_usd == 1_000_000
    assert cfg.events.top_alt_min_notional_usd == 500_000
    assert cfg.events.other_min_notional_usd == 250_000
    assert cfg.top_altcoins
    assert cfg.ticker_symbols
    assert cfg.core_wallets == []
    assert set(cfg.protocol_excluded_addresses) == HLP_ADDRESSES
    assert cfg.health.hl_rest.stale_after_s > 0
    assert cfg.health.hl_trades_ws.stale_after_s > 0
    assert cfg.health.hl_core_ws.stale_after_s > 0
    assert cfg.health.hl_leaderboard.stale_after_s > cfg.frequencies.leaderboard_interval_s
    assert cfg.health.hl_profile.stale_after_s > cfg.frequencies.profile_interval_s


def test_hyperliquid_unknown_key_reports_chinese(tmp_path):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        "hyperliquid:\n  monitor_budegt_per_min: 900\n", encoding="utf-8"
    )

    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)

    message = str(exc.value)
    assert "hyperliquid.monitor_budegt_per_min" in message
    assert "未知配置项" in message


@pytest.mark.parametrize(
    "field",
    [
        "monitor_budget_per_min",
        "batch_budget_per_min",
        "discovery_query_limit_per_min",
        "rest_burst_window_s",
        "aimd_min_budget_per_min",
        "aimd_recovery_interval_s",
    ],
)
@pytest.mark.parametrize("value", [0, -1])
def test_hyperliquid_budget_must_be_positive_and_reports_chinese(tmp_path, field, value):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        f"hyperliquid:\n  {field}: {value}\n", encoding="utf-8"
    )

    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)

    message = str(exc.value)
    assert f"hyperliquid.{field}" in message
    assert "必须大于 0" in message


@pytest.mark.parametrize(
    "override, field",
    [
        ("reconcile_concurrency: 0", "reconcile_concurrency"),
        ("frequencies:\n    hot_reconcile_interval_s: 0", "frequencies.hot_reconcile_interval_s"),
        ("frequencies:\n    trade_max_wait_s: 0", "frequencies.trade_max_wait_s"),
        ("frequencies:\n    hot_reconcile_aging_s: 0", "frequencies.hot_reconcile_aging_s"),
        ("frequencies:\n    normal_reconcile_aging_s: 0", "frequencies.normal_reconcile_aging_s"),
        (
            "frequencies:\n    deferred_fill_lookup_timeout_s: 0",
            "frequencies.deferred_fill_lookup_timeout_s",
        ),
        (
            "frequencies:\n    discovery_summary_interval_s: 0",
            "frequencies.discovery_summary_interval_s",
        ),
        ("tiers:\n    focus_limit: 0", "tiers.focus_limit"),
        ("events:\n    a_level_notional_usd: 0", "events.a_level_notional_usd"),
        ("health:\n    hl_rest:\n      stale_after_s: 0", "health.hl_rest.stale_after_s"),
    ],
)
def test_hyperliquid_numeric_settings_must_be_positive(tmp_path, override, field):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        f"hyperliquid:\n  {override}\n", encoding="utf-8"
    )

    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)

    message = str(exc.value)
    assert f"hyperliquid.{field}" in message
    assert "必须大于 0" in message


def test_hyperliquid_core_wallets_are_normalized_to_lowercase(tmp_path):
    base = _copy_default(tmp_path)
    mixed_case = "0xAbCdEf0123456789aBCDef0123456789abCDef01"
    (base / "config.local.yaml").write_text(
        f"hyperliquid:\n  core_wallets: [{mixed_case}]\n", encoding="utf-8"
    )

    settings = load_settings(base, environ=ENV)

    assert settings.hyperliquid.core_wallets == [mixed_case.lower()]


@pytest.mark.parametrize(
    "wallet",
    [
        "abcdef0123456789abcdef0123456789abcdef01",
        "0xabcdef",
        "0xgggggggggggggggggggggggggggggggggggggggg",
        " 0xabcdef0123456789abcdef0123456789abcdef01 ",
    ],
)
def test_hyperliquid_invalid_core_wallet_reports_chinese(tmp_path, wallet):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        f'hyperliquid:\n  core_wallets: ["{wallet}"]\n', encoding="utf-8"
    )

    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)

    message = str(exc.value)
    assert "hyperliquid.core_wallets" in message
    assert "核心钱包地址必须是 0x 开头的 40 位十六进制地址" in message


def test_hyperliquid_duplicate_core_wallet_reports_chinese(tmp_path):
    base = _copy_default(tmp_path)
    lower = "0xabcdef0123456789abcdef0123456789abcdef01"
    upper = "0xABCDEF0123456789ABCDEF0123456789ABCDEF01"
    (base / "config.local.yaml").write_text(
        f"hyperliquid:\n  core_wallets: [{lower}, {upper}]\n", encoding="utf-8"
    )

    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)

    message = str(exc.value)
    assert "hyperliquid.core_wallets" in message
    assert "核心钱包地址不能重复" in message


def test_hyperliquid_more_than_ten_core_wallets_reports_chinese(tmp_path):
    base = _copy_default(tmp_path)
    wallets = [f"0x{number:040x}" for number in range(11)]
    (base / "config.local.yaml").write_text(
        "hyperliquid:\n  core_wallets:\n" + "".join(f"    - {wallet}\n" for wallet in wallets),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)

    message = str(exc.value)
    assert "hyperliquid.core_wallets" in message
    assert "核心钱包最多配置 10 个" in message


@pytest.mark.parametrize(
    "source, interval_field",
    [("hl_leaderboard", "leaderboard_interval_s"), ("hl_profile", "profile_interval_s")],
)
def test_hyperliquid_low_frequency_health_threshold_exceeds_period(
    tmp_path, source, interval_field
):
    base = _copy_default(tmp_path)
    (base / "config.local.yaml").write_text(
        "hyperliquid:\n"
        "  frequencies:\n"
        f"    {interval_field}: 100\n"
        "  health:\n"
        f"    {source}:\n"
        "      stale_after_s: 100\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc:
        load_settings(base, environ=ENV)

    message = str(exc.value)
    assert "hyperliquid" in message
    assert source in message
    assert "必须大于正常周期" in message
