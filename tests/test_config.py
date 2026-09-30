from pathlib import Path

import pytest

from tidewatch.config import ConfigError, deep_merge, load_settings

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
    assert "collectors.jin10.interval" in str(exc.value) and "未知配置项" in str(exc.value)
