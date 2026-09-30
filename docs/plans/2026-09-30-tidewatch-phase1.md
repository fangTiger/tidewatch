# 观潮一期（核心流水线）实施计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 跑通「采集 → 落库去重 → 分级 → 聚簇 → 推送决策 → 飞书推送 → 网站实时展示」全链路，接入金十快讯、币安公告、OKX 公告三个数据源。

**Architecture:** Python 3.11 asyncio 单进程。采集器把 `FetchResult` 交给唯一串行入口 `EventSink`，`EventSink` 在一个 PostgreSQL 事务内完成插入去重、规则分级、聚簇、推送决策与游标推进；提交后由 `Dispatcher` 按推送状态机发送到飞书，网站通过 SSE 实时展示。

**Tech Stack:** Python 3.11、SQLAlchemy 2 Core（asyncpg）、Alembic、pydantic v2、httpx、websockets 15、FastAPI + Jinja2 + sse-starlette、pytest + pytest-asyncio + respx。

**关联文档：**
- 系统设计：`docs/plans/2026-09-30-tidewatch-design.md`
- OpenSpec 变更：`openspec/changes/add-phase1-core-pipeline/`（proposal / design / specs / tasks）

**通用约定（每个任务都适用）：**
- 所有命令在仓库根目录 `/Users/captain/python/Claude/news` 执行，执行前先 `source .venv/bin/activate`（任务 1 创建）。
- 在分支 `feat/phase1-core-pipeline` 上工作（已创建，测试夹具、OpenSpec 变更与本计划已提交）；**不要使用 git worktree**——`.env` 只存在于主目录。
- 代码注释、文档字符串、日志消息用中文；变量、函数、类名用英文。
- 本地 `.env` 已存在（含数据库连接，已被 git 忽略），不要提交它；测试库 `tidewatch_test` 已创建。
- 访问海外接口需要代理 `http://127.0.0.1:10808`；金十直连。
- 完成任务后，在 `openspec/changes/add-phase1-core-pipeline/tasks.md` 中勾选对应条目（`- [ ]` → `- [x]`），与代码一起提交。
- 每个任务最后一步提交一次，提交信息使用下列格式（第二个 `-m` 为署名行）：
  `git commit -m "<类型>: <中文说明>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`

---

## 文件结构

| 文件 | 职责 |
|---|---|
| `pyproject.toml` | 依赖、打包、pytest 配置 |
| `config.yaml` / `config.local.example.yaml` / `.env.example` | 公开默认配置 / 私有配置示例 / 密钥示例 |
| `alembic.ini`、`migrations/` | 数据库迁移 |
| `tidewatch/__init__.py` | 版本号 |
| `tidewatch/__main__.py` | 命令行入口（`--only`、`--dry-run`） |
| `tidewatch/app.py` | 组件装配、启动与优雅退出 |
| `tidewatch/config.py` | 三层配置加载、深度合并、校验与中文错误 |
| `tidewatch/logging_setup.py` | 按天轮转的中文日志 |
| `tidewatch/clock.py` | UTC 时钟与本地时区常量（便于测试注入时间） |
| `tidewatch/models.py` | `Event`、`FetchResult`、`RuleResult`、等级工具函数 |
| `tidewatch/store/tables.py` | 四张表的 SQLAlchemy Core 定义 |
| `tidewatch/store/db.py` | 异步引擎创建 |
| `tidewatch/store/repo.py` | 事件插入、分级回写、游标读写等通用 SQL |
| `tidewatch/pipeline/normalize.py` | 标题规范化、HTML 清理、标题相似度、关注资产识别 |
| `tidewatch/pipeline/rules/keywords.py` | 关键词组合匹配（all / any / none） |
| `tidewatch/pipeline/rules/engine.py` | `RuleEngine`、`RuleContext` |
| `tidewatch/pipeline/rules/news.py` | 金十快讯分级规则 |
| `tidewatch/pipeline/rules/exchange.py` | 交易所公告分级规则 |
| `tidewatch/pipeline/cluster.py` | 事件聚簇（标题相似度、跨类显式匹配） |
| `tidewatch/pipeline/decision.py` | 以簇为单位的推送决策（写 `push_log`） |
| `tidewatch/pipeline/sink.py` | `EventSink` 串行命令队列与事务流程 |
| `tidewatch/notify/base.py` | `Notifier` 接口、`EventCard`、`SendResult` |
| `tidewatch/notify/dryrun.py` | 演练通知器 |
| `tidewatch/notify/feishu.py` | 飞书自定义机器人通知器 |
| `tidewatch/notify/dispatcher.py` | 发送队列、防刷屏、汇总、重试与期限 |
| `tidewatch/health.py` | 采集器健康监控与告警 |
| `tidewatch/collectors/base.py` | `TokenBucket`、`Backoff`、`PollCollector`、`StreamCollector` |
| `tidewatch/collectors/http.py` | 按代理设置创建 httpx 客户端 |
| `tidewatch/collectors/announcements.py` | 公告分类、标题中的币种提取、币安去重键 |
| `tidewatch/collectors/jin10.py` | 金十快讯采集器 |
| `tidewatch/collectors/okx_ann.py` | OKX 公告采集器 |
| `tidewatch/collectors/binance_ann.py` | 币安公告采集器（WS 与降级轮询） |
| `tidewatch/web/broadcaster.py` | 进程内 SSE 广播 |
| `tidewatch/web/queries.py` | 时间线与状态页查询 |
| `tidewatch/web/server.py` | FastAPI 应用与路由 |
| `tidewatch/web/templates/*.html`、`tidewatch/web/static/*` | 页面模板与前端脚本样式 |
| `deploy/com.captain.tidewatch.plist`、`deploy/install.sh`、`deploy/uninstall.sh` | launchd 守护 |
| `README.md` | 中文使用说明（安装、配置、运行、launchd） |
| `tests/…` | 各模块测试；`tests/fixtures/` 为实测录制的接口样本（已存在） |

已存在的测试夹具（2026-09-30 实测录制，勿修改）：
- `tests/fixtures/jin10/flash_page1.json`：金十第一页 20 条，含 VIP 锁定条目、`type=2` 文章条目、6 条 `important=1`
- `tests/fixtures/okx/announcements_page1.json`：OKX 公告第一页 20 条
- `tests/fixtures/binance/list_48.json`、`list_161.json`：币安新币上线（48）与下架（161）列表各 10 条

---

## Chunk 1: 项目骨架、配置与日志

### Task 1: 项目骨架与依赖

**Files:**
- Create: `pyproject.toml`
- Create: `tidewatch/__init__.py`
- Test: `tests/test_package.py`

- [ ] **Step 1: 创建虚拟环境与空包目录**

Run:
```bash
python3 -m venv .venv && source .venv/bin/activate && python --version
mkdir -p tidewatch && touch tidewatch/__init__.py
```
Expected: `Python 3.11.x`

- [ ] **Step 2: 写 `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "tidewatch"
version = "0.1.0"
description = "观潮：个人市场监控系统"
requires-python = ">=3.11"
dependencies = [
  "httpx>=0.27",
  "websockets>=15.0",
  "fastapi>=0.115",
  "uvicorn>=0.30",
  "sse-starlette>=2.1",
  "jinja2>=3.1",
  "sqlalchemy[asyncio]>=2.0.30",
  "asyncpg>=0.29",
  "alembic>=1.13",
  "pydantic>=2.7",
  "pyyaml>=6.0",
  "python-dotenv>=1.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-asyncio>=0.24", "respx>=0.22"]

[tool.hatch.build.targets.wheel]
packages = ["tidewatch"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
testpaths = ["tests"]
```

- [ ] **Step 3: 安装依赖**

Run（走代理）：
```bash
HTTPS_PROXY=http://127.0.0.1:10808 pip install -U pip && HTTPS_PROXY=http://127.0.0.1:10808 pip install -e ".[dev]"
```
如代理不可用，改用清华镜像：
```bash
pip install -U pip -i https://pypi.tuna.tsinghua.edu.cn/simple && pip install -e ".[dev]" -i https://pypi.tuna.tsinghua.edu.cn/simple
```
Expected: 最后一行为 `Successfully installed ...`（包含 tidewatch、fastapi、sqlalchemy、pytest 等）

- [ ] **Step 4: 写失败的测试 `tests/test_package.py`**

```python
import tidewatch


def test_version():
    assert tidewatch.__version__ == "0.1.0"
```

- [ ] **Step 5: 运行测试确认失败**

Run: `pytest tests/test_package.py -v`
Expected: FAIL，`AttributeError: module 'tidewatch' has no attribute '__version__'`

- [ ] **Step 6: 写 `tidewatch/__init__.py`**

```python
"""观潮（Tidewatch）：个人市场监控系统"""

__version__ = "0.1.0"
```

- [ ] **Step 7: 运行测试**

Run: `pytest tests/test_package.py -v`
Expected: `1 passed`

- [ ] **Step 8: 提交**

```bash
git add pyproject.toml tidewatch/__init__.py tests/test_package.py
git commit -m "chore: 初始化项目骨架与依赖" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: 配置加载

**Files:**
- Create: `config.yaml`、`config.local.example.yaml`、`.env.example`
- Create: `tidewatch/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: 写默认配置 `config.yaml`**

```yaml
# 观潮默认配置（公开提交）。个人配置写在 config.local.yaml，密钥写在 .env
proxy_url: null  # 在 config.local.yaml 中设置，例如 http://127.0.0.1:10808

watchlist:
  assets:
    - {symbol: BTC, aliases: [比特币, Bitcoin]}
    - {symbol: ETH, aliases: [以太坊, Ethereum]}
    - {symbol: SOL, aliases: [Solana]}
    - {symbol: XRP, aliases: [瑞波, Ripple]}
    - {symbol: HYPE, aliases: [Hyperliquid]}
    - {symbol: MSTR, aliases: [微策略, MicroStrategy]}
    - {symbol: COIN, aliases: [Coinbase]}
    - {symbol: NVDA, aliases: [英伟达, NVIDIA, Nvidia]}
    - {symbol: TSLA, aliases: [特斯拉, Tesla]}
    - {symbol: NDX, aliases: [纳指, 纳斯达克, Nasdaq]}
    - {symbol: SPX, aliases: [标普, S&P 500]}

collectors:
  jin10:
    interval_s: 20
    proxy: direct
    stale_after_s: 300
    backfill_max_s: 21600
    max_push_age_s: 1800
    rate_per_s: 2
  binance_ann:
    interval_s: 30          # 无 API Key 时的降级轮询间隔
    proxy: global
    stale_after_s: 180
    backfill_max_s: 21600
    max_push_age_s: 1800
    gap_fill_interval_s: 600
    rate_per_s: 1
  okx_ann:
    interval_s: 30
    proxy: global
    stale_after_s: 300
    backfill_max_s: 21600
    max_push_age_s: 1800
    rate_per_s: 1

rules:
  news:
    s_keywords:
      - {id: s.rate_decision, any: [利率决议, 议息会议, FOMC], none: [前瞻, 展望]}
      - {id: s.powell, all: [鲍威尔], any: [讲话, 表示, 称]}
      - {id: s.cpi, any: [CPI], none: [前瞻, 展望]}
      - {id: s.pce, any: [PCE], none: [前瞻, 展望]}
      - {id: s.nfp, any: [非农], none: [前瞻, 展望]}
      - {id: s.emergency_rate, any: [紧急降息, 紧急加息]}
      - {id: s.tariff, all: [关税], any: [中国, 全面, 对等]}
      - {id: s.etf_approval, all: [ETF], any: [批准, 获批]}
      - {id: s.exchange_hack, any: [交易所被盗, 遭黑客攻击, 被黑客攻击]}
      - {id: s.depeg, any: [脱锚]}
      - {id: s.withdraw_halt, any: [暂停提现, 暂停提币]}
      - {id: s.bankruptcy, any: [破产]}
    a_keywords:
      - {id: a.macro, any: [美联储, 央行, 降息, 加息, 关税, 制裁]}
      - {id: a.regulator, any: [SEC, 证监会]}
      - {id: a.etf, any: [ETF]}
      - {id: a.geo, any: [战争, 停火]}
      - {id: a.security, any: [黑客, 被盗]}
      - {id: a.bankruptcy, any: [破产]}
    people: [特朗普, 马斯克]
  exchange:
    exchange_names: [币安, Binance, OKX, 欧易]
    listing_keywords: [上线, 上币, list, 下架, delist]

pipeline:
  cluster_window_s: 600
  title_similarity: 0.5

dispatch:
  a_window_s: 300
  a_window_max: 10
  digest_interval_s: 60
  retry_immediate: 3
  retry_interval_s: 60
  retry_max_s: 86400
  health_alert_interval_s: 3600

web:
  host: 127.0.0.1
  port: 8000
  timeline_limit: 200

ai:
  enabled: false

log:
  dir: log
  retention_days: 14
  level: INFO
```

- [ ] **Step 2: 写 `config.local.example.yaml` 与 `.env.example`**

`config.local.example.yaml`：
```yaml
# 复制为 config.local.yaml 后修改（该文件被 .gitignore 排除，不会提交）
proxy_url: http://127.0.0.1:10808
watchlist:
  assets:
    # 以 symbol 合并：写已有 symbol 会覆盖其别名，写新 symbol 即新增关注资产
    - {symbol: DOGE, aliases: [狗狗币, Dogecoin]}
    # 删除某个默认关注资产
    - {symbol: TSLA, remove: true}
```

`.env.example`：
```bash
# 复制为 .env 后填写（该文件被 .gitignore 排除，不会提交）
TIDEWATCH_DATABASE_URL=postgresql+asyncpg://用户名:密码@localhost:5432/tidewatch
FEISHU_WEBHOOK=
FEISHU_SECRET=
BINANCE_API_KEY=
BINANCE_API_SECRET=
```

- [ ] **Step 3: 写失败的测试 `tests/test_config.py`**

```python
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
```

- [ ] **Step 4: 运行测试确认失败**

Run: `pytest tests/test_config.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.config'`

- [ ] **Step 5: 实现 `tidewatch/config.py`**

```python
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
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


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
    database_url: str
    feishu_webhook: str | None = None
    feishu_secret: str | None = None
    binance_api_key: str | None = None
    binance_api_secret: str | None = None

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
    secrets: Secrets

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
        return f"取值必须为 {ctx.get('expected')}"
    if kind == "value_error":
        return str(ctx.get("error", err["msg"]))
    return f"{err['msg']}（输入：{err.get('input')!r}）"
```

- [ ] **Step 6: 运行测试**

Run: `pytest tests/test_config.py -v`
Expected: `9 passed`

- [ ] **Step 7: 提交**

```bash
git add config.yaml config.local.example.yaml .env.example tidewatch/config.py tests/test_config.py
git commit -m "feat: 三层配置加载与中文校验错误" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: 日志初始化

**Files:**
- Create: `tidewatch/clock.py`
- Create: `tidewatch/logging_setup.py`
- Test: `tests/test_logging_setup.py`

- [ ] **Step 1: 写失败的测试 `tests/test_logging_setup.py`**

```python
import logging
from datetime import date

from tidewatch.logging_setup import DailyFileHandler


def _record(message: str) -> logging.LogRecord:
    return logging.LogRecord("t", logging.INFO, __file__, 1, message, None, None)


def test_daily_file_naming_and_cleanup(tmp_path):
    (tmp_path / "tidewatch_2026-09-01.log").write_text("old", encoding="utf-8")
    (tmp_path / "tidewatch_2026-09-20.log").write_text("keep", encoding="utf-8")
    today = {"value": date(2026, 9, 30)}
    handler = DailyFileHandler(tmp_path, "tidewatch", retention_days=14, today=lambda: today["value"])
    handler.setFormatter(logging.Formatter("%(message)s"))

    handler.emit(_record("第一条日志"))
    assert (tmp_path / "tidewatch_2026-09-30.log").read_text(encoding="utf-8") == "第一条日志\n"
    assert not (tmp_path / "tidewatch_2026-09-01.log").exists()
    assert (tmp_path / "tidewatch_2026-09-20.log").exists()

    today["value"] = date(2026, 10, 1)
    handler.emit(_record("第二天"))
    assert (tmp_path / "tidewatch_2026-10-01.log").read_text(encoding="utf-8") == "第二天\n"
    handler.close()


def test_setup_logging_writes_file_and_stdout(tmp_path):
    import sys

    from tidewatch.config import LogCfg
    from tidewatch.logging_setup import setup_logging

    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    try:
        setup_logging(LogCfg(dir="log", retention_days=14, level="INFO"), tmp_path)
        logging.getLogger("tidewatch.test").info("中文日志")
        files = list((tmp_path / "log").glob("tidewatch_*.log"))
        assert len(files) == 1 and "中文日志" in files[0].read_text(encoding="utf-8")
        assert any(isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout for h in root.handlers)
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        for handler in saved_handlers:
            root.addHandler(handler)
        root.setLevel(saved_level)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_logging_setup.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.logging_setup'`

- [ ] **Step 3: 实现 `tidewatch/logging_setup.py`**

```python
"""日志初始化

日志按天写入 log/tidewatch_YYYY-MM-DD.log（按上海时区换日），保留 N 天，同时输出到标准输出。
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, TextIO

from tidewatch.clock import LOCAL_TZ
from tidewatch.config import LogCfg

_FORMAT = "%(asctime)s %(levelname)s %(name)s：%(message)s"


def _local_today() -> date:
    return datetime.now(LOCAL_TZ).date()


class DailyFileHandler(logging.Handler):
    """按天切换日志文件的处理器，文件名形如 {prefix}_YYYY-MM-DD.log"""

    def __init__(
        self,
        directory: Path | str,
        prefix: str,
        retention_days: int,
        today: Callable[[], date] = _local_today,
    ) -> None:
        super().__init__()
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._prefix = prefix
        self._retention_days = retention_days
        self._today = today
        self._current: date | None = None
        self._stream: TextIO | None = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            today = self._today()
            if today != self._current:
                self._rollover(today)
            assert self._stream is not None
            self._stream.write(self.format(record) + "\n")
            self._stream.flush()
        except Exception:
            self.handleError(record)

    def _rollover(self, today: date) -> None:
        if self._stream is not None:
            self._stream.close()
        self._current = today
        self._stream = open(self._dir / f"{self._prefix}_{today.isoformat()}.log", "a", encoding="utf-8")
        self._cleanup(today)

    def _cleanup(self, today: date) -> None:
        """删除超过保留天数的日志文件（保留今天在内的 retention_days 天）"""
        cutoff = today - timedelta(days=self._retention_days - 1)
        for path in self._dir.glob(f"{self._prefix}_*.log"):
            try:
                day = date.fromisoformat(path.stem[len(self._prefix) + 1 :])
            except ValueError:
                continue
            if day < cutoff:
                path.unlink(missing_ok=True)

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        super().close()


def setup_logging(cfg: LogCfg, base_dir: Path | str) -> None:
    """配置根日志：按天文件 + 标准输出"""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(cfg.level)
    formatter = logging.Formatter(_FORMAT)
    # 时间戳统一用上海时间，与日志文件的换日时区一致
    formatter.converter = lambda ts: datetime.fromtimestamp(ts, LOCAL_TZ).timetuple()
    file_handler = DailyFileHandler(Path(base_dir) / cfg.dir, "tidewatch", cfg.retention_days)
    stream_handler = logging.StreamHandler(sys.stdout)
    for handler in (file_handler, stream_handler):
        handler.setFormatter(formatter)
        root.addHandler(handler)
    # 第三方库的请求日志过于频繁，只保留警告以上
    for noisy in ("httpx", "httpcore", "websockets", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
```

同时创建 `tidewatch/clock.py`（`logging_setup` 依赖它；后续任务也会使用）：

```python
"""时间工具：统一获取 UTC 当前时间，组件通过注入 clock 便于测试"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable
from zoneinfo import ZoneInfo

Clock = Callable[[], datetime]

# 页面、日志、推送展示使用的本地时区
LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def utcnow() -> datetime:
    """返回带时区的 UTC 当前时间"""
    return datetime.now(timezone.utc)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_logging_setup.py -v`
Expected: `2 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/logging_setup.py tidewatch/clock.py tests/test_logging_setup.py
git commit -m "feat: 按天轮转的中文日志" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 2: 存储与迁移

### Task 4: 表定义与测试数据库夹具

**Files:**
- Create: `tidewatch/store/__init__.py`（空文件）
- Create: `tidewatch/store/tables.py`
- Create: `tidewatch/store/db.py`
- Create: `tests/conftest.py`
- Test: `tests/test_tables.py`

- [ ] **Step 1: 创建包目录**

Run: `mkdir -p tidewatch/store && touch tidewatch/store/__init__.py`

- [ ] **Step 2: 写测试夹具 `tests/conftest.py`**

测试库连接串优先取环境变量 `TIDEWATCH_TEST_DATABASE_URL`，否则把 `.env` 中 `TIDEWATCH_DATABASE_URL` 的库名替换为 `tidewatch_test`（避免把本地密码写进公开仓库）。

```python
"""测试公共夹具"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pytest
from dotenv import dotenv_values
from sqlalchemy import text
from sqlalchemy.engine import make_url

from tidewatch.config import load_settings
from tidewatch.store.db import create_db_engine
from tidewatch.store.tables import metadata

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _test_db_url() -> str | None:
    url = os.environ.get("TIDEWATCH_TEST_DATABASE_URL")
    if url:
        return url
    base = dotenv_values(REPO_ROOT / ".env").get("TIDEWATCH_DATABASE_URL")
    if not base:
        return None
    return base.rsplit("/", 1)[0] + "/tidewatch_test"


TEST_DB_URL = _test_db_url()
_schema_ready = False


@pytest.fixture
async def engine():
    """每个测试一个引擎；首次使用时重建表结构，之后每次清空数据"""
    global _schema_ready
    if TEST_DB_URL is None:
        pytest.skip("未配置测试数据库：请设置 TIDEWATCH_TEST_DATABASE_URL 或 .env 中的 TIDEWATCH_DATABASE_URL")
    # 防止误把正式库当测试库：库名必须以 _test 结尾
    assert make_url(TEST_DB_URL).database.endswith("_test"), "测试库名必须以 _test 结尾"
    eng = create_db_engine(TEST_DB_URL)
    async with eng.begin() as conn:
        if not _schema_ready:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)
            _schema_ready = True
        await conn.execute(
            text("TRUNCATE push_log, events, event_clusters, collector_state RESTART IDENTITY CASCADE")
        )
    yield eng
    await eng.dispose()


@pytest.fixture
def settings(tmp_path):
    """仓库默认配置：只复制 config.yaml，不合并本地 config.local.yaml；数据库指向测试库"""
    (tmp_path / "config.yaml").write_text((REPO_ROOT / "config.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    return load_settings(tmp_path, environ={"TIDEWATCH_DATABASE_URL": TEST_DB_URL or "postgresql+asyncpg://x/y"})


class FakeClock:
    """可手动推进的时钟"""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        from datetime import timedelta

        self.now = self.now + timedelta(**kwargs)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
```

注意：`settings` 夹具只使用仓库的 `config.yaml`（复制到临时目录后加载），不会合并本地 `config.local.yaml`，因此测试结果不受个人配置影响。

- [ ] **Step 3: 写测试 `tests/test_tables.py`**

```python
from datetime import datetime, timezone

import pytest
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from tidewatch.store.tables import event_clusters, events


def _event_row(key: str) -> dict:
    now = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
    return {
        "source": "jin10",
        "category": "news",
        "event_type": "flash",
        "occurred_at": now,
        "collected_at": now,
        "title": "测试标题",
        "assets": ["BTC", "ETH"],
        "dedupe_key": key,
        "raw": {"important": 1},
    }


async def test_insert_and_read_back(engine):
    async with engine.begin() as conn:
        await conn.execute(insert(events).values(**_event_row("k1")))
        row = (await conn.execute(select(events).where(events.c.dedupe_key == "k1"))).mappings().one()
    assert row["assets"] == ["BTC", "ETH"]
    assert row["raw"] == {"important": 1}
    assert row["body"] == ""
    assert row["occurred_at"].tzinfo is not None


async def test_dedupe_key_unique(engine):
    async with engine.begin() as conn:
        await conn.execute(insert(events).values(**_event_row("dup")))
    with pytest.raises(IntegrityError):
        async with engine.begin() as conn:
            await conn.execute(insert(events).values(**_event_row("dup")))


async def test_cluster_defaults(engine):
    now = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
    async with engine.begin() as conn:
        cid = (
            await conn.execute(
                insert(event_clusters)
                .values(category="news", first_seen_at=now, last_seen_at=now, max_level="B")
                .returning(event_clusters.c.id)
            )
        ).scalar_one()
        row = (await conn.execute(select(event_clusters).where(event_clusters.c.id == cid))).mappings().one()
    assert row["member_count"] == 1 and row["source_count"] == 1
    assert row["committed_level"] is None and row["note_sent"] is False
```

- [ ] **Step 4: 运行测试确认失败**

Run: `pytest tests/test_tables.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.store.db'`（conftest 导入失败）

- [ ] **Step 5: 实现 `tidewatch/store/tables.py`**

```python
"""数据库表定义（SQLAlchemy Core）

一期四张表：events（统一事件）、event_clusters（事件簇与推送状态）、
collector_state（采集器游标与健康）、push_log（推送记录）。
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

metadata = MetaData()

event_clusters = Table(
    "event_clusters",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("category", Text, nullable=False),
    Column("cluster_key", Text),
    Column("first_event_id", BigInteger),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("max_level", String(1), nullable=False),
    Column("committed_level", String(1)),
    Column("delivered_level", String(1)),
    Column("rep_event_id", BigInteger),
    Column("member_count", Integer, nullable=False, server_default=text("1")),
    Column("source_count", Integer, nullable=False, server_default=text("1")),
    Column("note_sent", Boolean, nullable=False, server_default=text("false")),
)
Index("ix_event_clusters_category_first_seen", event_clusters.c.category, event_clusters.c.first_seen_at)

events = Table(
    "events",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("source", Text, nullable=False),
    Column("category", Text, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("ref_date", Date),
    Column("collected_at", DateTime(timezone=True), nullable=False),
    Column("title", Text, nullable=False),
    Column("body", Text, nullable=False, server_default=text("''")),
    Column("url", Text),
    Column("assets", ARRAY(Text), nullable=False, server_default=text("'{}'")),
    Column("amount_usd", Numeric),
    Column("rule_level", String(1)),
    Column("rule_hits", ARRAY(Text), nullable=False, server_default=text("'{}'")),
    Column("ai_status", Text),
    Column("ai_level", String(1)),
    Column("ai_direction", Text),
    Column("ai_summary", Text),
    Column("ai_runner", Text),
    Column("ai_at", DateTime(timezone=True)),
    Column("final_level", String(1)),
    Column("dedupe_key", Text, nullable=False, unique=True),
    Column("cluster_id", BigInteger, ForeignKey("event_clusters.id")),
    Column("raw", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
)
Index("ix_events_collected_at", events.c.collected_at)
Index("ix_events_occurred_at", events.c.occurred_at)
Index("ix_events_cluster_id", events.c.cluster_id)

collector_state = Table(
    "collector_state",
    metadata,
    Column("name", Text, primary_key=True),
    Column("cursor", JSONB),
    Column("status", Text, nullable=False, server_default=text("'unknown'")),
    Column("mode", Text, nullable=False, server_default=text("'normal'")),
    Column("last_success_at", DateTime(timezone=True)),
    Column("last_event_at", DateTime(timezone=True)),
    Column("last_error_at", DateTime(timezone=True)),
    Column("last_error", Text),
    Column("consecutive_failures", Integer, nullable=False, server_default=text("0")),
    Column("last_alert_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
)

push_log = Table(
    "push_log",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("cluster_id", BigInteger, ForeignKey("event_clusters.id"), nullable=False),
    Column("event_id", BigInteger, ForeignKey("events.id"), nullable=False),
    Column("level", String(1), nullable=False),
    Column("kind", Text, nullable=False),  # event / upgrade
    Column("batch_id", Text),
    Column("status", Text, nullable=False),  # pending/ok/failed/expired/superseded/dry_run
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("retry_until", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("sent_at", DateTime(timezone=True)),
    Column("response", Text),
)
Index("ix_push_log_status", push_log.c.status)
Index("ix_push_log_cluster_id", push_log.c.cluster_id)
```

- [ ] **Step 6: 实现 `tidewatch/store/db.py`**

```python
"""数据库引擎"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_db_engine(url: str, *, echo: bool = False) -> AsyncEngine:
    """创建异步引擎（asyncpg 驱动），连接串形如 postgresql+asyncpg://用户:密码@主机:端口/库名"""
    return create_async_engine(url, pool_size=5, max_overflow=5, pool_pre_ping=True, echo=echo)
```

- [ ] **Step 7: 运行测试**

Run: `pytest tests/test_tables.py -v`
Expected: `3 passed`（若显示 skipped，说明未读到数据库连接，检查 `.env`）

- [ ] **Step 8: 全量回归**

Run: `pytest -q`
Expected: 全部通过（0 failed）

- [ ] **Step 9: 提交**

```bash
git add tidewatch/store tests/conftest.py tests/test_tables.py
git commit -m "feat: 一期数据表定义与测试数据库夹具" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Alembic 迁移

**Files:**
- Create: `alembic.ini`、`migrations/env.py`、`migrations/script.py.mako`、`migrations/versions/<自动生成>_phase1_initial_tables.py`

- [ ] **Step 1: 生成 Alembic 脚手架**

Run: `alembic init -t async migrations`
Expected: 生成 `alembic.ini`、`migrations/env.py`、`migrations/script.py.mako`、`migrations/README`

- [ ] **Step 2: 修改 `alembic.ini`**

把 `sqlalchemy.url = driver://user:pass@localhost/dbname` 这一行改为（留空，由 env.py 从 `.env` 读取）：
```ini
sqlalchemy.url =
```

- [ ] **Step 3: 用以下内容整体替换 `migrations/env.py`**

```python
"""Alembic 迁移环境：连接串默认从 .env 的 TIDEWATCH_DATABASE_URL 读取"""

import asyncio
from logging.config import fileConfig
from pathlib import Path

from alembic import context

from tidewatch.config import load_settings
from tidewatch.store.db import create_db_engine
from tidewatch.store.tables import metadata

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata


def _database_url() -> str:
    # alembic.ini 中 sqlalchemy.url 非空时优先；否则读取仓库配置（可用环境变量 TIDEWATCH_DATABASE_URL 临时覆盖 .env）
    url = config.get_main_option("sqlalchemy.url")
    if url:
        return url
    return load_settings(Path(__file__).resolve().parents[1]).secrets.database_url


def run_migrations_offline() -> None:
    context.configure(url=_database_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_db_engine(_database_url())
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
```

- [ ] **Step 4: 自动生成初始迁移**

Run: `alembic revision --autogenerate -m "phase1 initial tables"`
Expected: 输出 `Detected added table 'event_clusters'`、`'events'`、`'collector_state'`、`'push_log'` 及各索引，生成 `migrations/versions/*_phase1_initial_tables.py`。
打开生成的文件确认：`upgrade()` 先建 `event_clusters` 再建 `events`、`push_log`；代码中以 `postgresql.ARRAY(...)`、`postgresql.JSONB(...)` 引用，文件头部须有 `from sqlalchemy.dialects import postgresql`（Alembic 通常会自动生成；若缺少则补上这一行）。

- [ ] **Step 5: 在 `tidewatch` 库执行迁移并验证可回滚**

Run:
```bash
alembic upgrade head && psql "$(grep ^TIDEWATCH_DATABASE_URL= .env | cut -d= -f2- | sed 's/+asyncpg//')" -c "\dt"
alembic downgrade base && alembic upgrade head
```
Expected: `\dt` 列出 `alembic_version`、`collector_state`、`event_clusters`、`events`、`push_log`；downgrade 与再次 upgrade 均无报错。

Run:
```bash
alembic check
psql "$(grep ^TIDEWATCH_DATABASE_URL= .env | cut -d= -f2- | sed 's/+asyncpg//')" -c "\d events"
```
Expected: `alembic check` 输出 `No new upgrade operations detected.`；`\d events` 显示 `events_dedupe_key_key` 唯一约束以及 `ix_events_collected_at`、`ix_events_occurred_at`、`ix_events_cluster_id` 三个索引。

- [ ] **Step 6: 提交**

```bash
git add alembic.ini migrations
git commit -m "feat: Alembic 迁移与一期初始表结构" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: 领域模型与等级工具

**Files:**
- Create: `tidewatch/models.py`
- Test: `tests/test_models.py`

- [ ] **Step 1: 写失败的测试 `tests/test_models.py`**

```python
from datetime import datetime, timezone

import pytest

from tidewatch.models import Event, FetchResult, RuleResult, level_ge, level_rank, max_level


def test_level_helpers():
    assert level_rank("S") > level_rank("A") > level_rank("B") > level_rank("C") > level_rank(None)
    assert level_ge("A", "B") and level_ge("A", "A") and not level_ge("B", "A")
    assert not level_ge(None, "C")
    assert max_level("B", None, "S", "A") == "S"
    assert max_level(None, None) is None


def test_event_requires_timezone():
    with pytest.raises(ValueError):
        Event(
            source="jin10",
            category="news",
            event_type="flash",
            occurred_at=datetime(2026, 9, 30, 10, 0),
            title="无时区",
            dedupe_key="jin10:1",
        )


def test_event_defaults():
    event = Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc),
        title="标题",
        dedupe_key="jin10:1",
    )
    assert event.assets == [] and event.raw == {} and event.body == ""
    assert event.id is None and event.rule_level is None
    result = FetchResult(events=[event], cursor={"last_id": "1"})
    assert result.records == []
    assert RuleResult(level="C").hits == []
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_models.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.models'`

- [ ] **Step 3: 实现 `tidewatch/models.py`**

```python
"""领域模型与等级工具"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

# 等级从低到高
LEVELS = ("C", "B", "A", "S")
_RANK = {level: index for index, level in enumerate(LEVELS)}


def level_rank(level: str | None) -> int:
    """等级序号：C=0 … S=3；None 为 -1"""
    return -1 if level is None else _RANK[level]


def level_ge(a: str | None, b: str) -> bool:
    """a 是否不低于 b"""
    return level_rank(a) >= level_rank(b)


def max_level(*levels: str | None) -> str | None:
    """取最高等级，全部为 None 时返回 None"""
    present = [level for level in levels if level is not None]
    return max(present, key=level_rank) if present else None


@dataclass
class Event:
    """统一事件：采集器负责基础字段，EventSink 负责分级、聚簇等字段"""

    source: str
    category: str
    event_type: str
    occurred_at: datetime
    title: str
    dedupe_key: str
    body: str = ""
    url: str | None = None
    assets: list[str] = field(default_factory=list)
    amount_usd: Decimal | float | None = None  # 数据库 Numeric 读回为 Decimal
    ref_date: date | None = None
    raw: dict[str, Any] = field(default_factory=dict)
    # 以下字段由 EventSink 填充
    id: int | None = None
    collected_at: datetime | None = None
    rule_level: str | None = None
    rule_hits: list[str] = field(default_factory=list)
    final_level: str | None = None
    ai_status: str | None = None
    cluster_id: int | None = None

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValueError(f"事件时间必须带时区：{self.dedupe_key}")


@dataclass
class FetchResult:
    """采集器一次拉取的结果"""

    events: list[Event]
    cursor: dict[str, Any] | None
    records: list[Any] = field(default_factory=list)  # 辅助记录（三期起使用）


@dataclass
class RuleResult:
    """规则分级结果"""

    level: str
    hits: list[str] = field(default_factory=list)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_models.py -v`
Expected: `3 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/models.py tests/test_models.py
git commit -m "feat: 统一事件模型与等级工具" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: 规范化工具（HTML 清理、标题相似度、关注资产识别）

**Files:**
- Create: `tidewatch/pipeline/__init__.py`（空文件）
- Create: `tidewatch/pipeline/normalize.py`
- Test: `tests/test_normalize.py`

标题相似度采用「规范化标题的字符二元组 Jaccard 相似度」。以下测试数据的相似度已实测：近乎重复 0.92、完全相同（仅标点不同）1.0、无关 0.05。

- [ ] **Step 1: 写失败的测试 `tests/test_normalize.py`**

```python
from tidewatch.config import AssetCfg
from tidewatch.pipeline.normalize import AssetMatcher, clean_text, contains_symbol, normalize_title, title_similarity

NFP = "美国9月非农就业人口增加8.2万人，预期增加12万人，前值增加14.2万人"


def test_clean_text_strips_html():
    assert clean_text("<b>美联储</b>&amp;加息  <br/>测试") == "美联储 &加息 测试"
    assert clean_text("") == ""


def test_normalize_title():
    assert normalize_title("Binance Will List ＸＹＺ (XYZ)!") == "binancewilllistxyzxyz"
    assert normalize_title("【港股】 恒指、翻红！") == "港股恒指翻红"


def test_title_similarity():
    assert title_similarity(NFP, f"【{NFP}】") == 1.0
    near = title_similarity(NFP, "美国9月非农就业人口增加8.2万人，预期12万人，前值增加14.2万人")
    assert near >= 0.9
    assert title_similarity("港股收盘：恒指涨1.2%，科指涨2%", "美股开盘：道指涨0.3%，纳指涨0.5%") < 0.1
    assert title_similarity("", "任意") == 0.0


def test_asset_matcher():
    matcher = AssetMatcher(
        [
            AssetCfg(symbol="BTC", aliases=["比特币", "Bitcoin"]),
            AssetCfg(symbol="ETH", aliases=["以太坊"]),
            AssetCfg(symbol="SOL", aliases=["Solana"]),
            AssetCfg(symbol="MSTR", aliases=["微策略"]),
        ]
    )
    assert matcher.find("微策略再次增持比特币，ETH 跟涨") == ["BTC", "ETH", "MSTR"]
    assert matcher.find("bitcoin 与 solana 齐涨") == ["BTC", "SOL"]
    # 全大写代码区分大小写且要求前后不是字母数字
    assert matcher.find("SOLUTION 与 ETHUSDT、eth") == []
    assert matcher.find("") == []
    assert matcher.find("ＢＴＣ 突破") == ["BTC"]  # 全角先转半角


def test_contains_symbol():
    assert contains_symbol("币安将上线XYZ", "XYZ")
    assert contains_symbol("Binance Will List XYZ (XYZ)", "XYZ")
    assert not contains_symbol("XYZW 上线", "XYZ")
    assert not contains_symbol("任意", "")
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mkdir -p tidewatch/pipeline && touch tidewatch/pipeline/__init__.py && pytest tests/test_normalize.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.pipeline.normalize'`

- [ ] **Step 3: 实现 `tidewatch/pipeline/normalize.py`**

```python
"""规范化工具：HTML 清理、标题规范化、标题相似度、关注资产识别"""

from __future__ import annotations

import html
import re
import unicodedata

from tidewatch.config import AssetCfg

_TAG = re.compile(r"<[^>]+>")
_NON_WORD = re.compile(r"[\W_]+", re.UNICODE)


def clean_text(text: str | None) -> str:
    """去掉 HTML 标签与实体，合并多余空白"""
    no_tags = _TAG.sub(" ", text or "")
    return " ".join(html.unescape(no_tags).split())


def normalize_title(text: str | None) -> str:
    """规范化：NFKC（全角转半角）、英文小写、去掉空白与标点"""
    return _NON_WORD.sub("", unicodedata.normalize("NFKC", text or "").lower())


def contains_symbol(text: str | None, symbol: str) -> bool:
    """文本中是否以独立形式出现币种代码（区分大小写；前后不紧挨字母或数字，紧挨中文算独立）"""
    if not symbol:
        return False
    content = unicodedata.normalize("NFKC", text or "")
    return re.search(rf"(?<![A-Za-z0-9]){re.escape(symbol)}(?![A-Za-z0-9])", content) is not None


def _bigrams(text: str | None) -> set[str]:
    norm = normalize_title(text)
    if len(norm) < 2:
        return {norm} if norm else set()
    return {norm[i : i + 2] for i in range(len(norm) - 1)}


def title_similarity(a: str | None, b: str | None) -> float:
    """两个标题的字符二元组 Jaccard 相似度（0~1）"""
    grams_a, grams_b = _bigrams(a), _bigrams(b)
    union = grams_a | grams_b
    return len(grams_a & grams_b) / len(union) if union else 0.0


class AssetMatcher:
    """根据关注资产的代码与别名在文本中识别资产

    - 全大写英文（如 BTC、SOL）：区分大小写，且前后不能紧挨字母或数字
    - 其他英文别名（如 Bitcoin）：不区分大小写，同样要求前后不紧挨字母或数字
    - 中文别名：按子串匹配
    """

    def __init__(self, assets: list[AssetCfg]) -> None:
        self._patterns: list[tuple[str, list[re.Pattern[str]]]] = [
            (asset.symbol, [self._compile(alias) for alias in (asset.symbol, *asset.aliases)])
            for asset in assets
        ]

    @staticmethod
    def _compile(alias: str) -> re.Pattern[str]:
        escaped = re.escape(alias)
        if alias.isascii():
            flags = 0 if alias.isupper() else re.IGNORECASE
            return re.compile(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", flags)
        return re.compile(escaped)

    def find(self, text: str | None) -> list[str]:
        """返回文本中出现的关注资产代码（按配置顺序、去重）；文本先做 NFKC（全角转半角）"""
        content = unicodedata.normalize("NFKC", text or "")
        return [symbol for symbol, patterns in self._patterns if any(p.search(content) for p in patterns)]
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_normalize.py -v && pytest -q`
Expected: `5 passed`；全量回归全部通过（0 failed）

- [ ] **Step 5: 提交**

```bash
git add tidewatch/pipeline tests/test_normalize.py
git commit -m "feat: 标题规范化、相似度与关注资产识别" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 3: 规则引擎

### Task 8: 关键词匹配、规则引擎与新闻规则

**Files:**
- Create: `tidewatch/pipeline/rules/__init__.py`（空文件）
- Create: `tidewatch/pipeline/rules/keywords.py`
- Create: `tidewatch/pipeline/rules/engine.py`
- Create: `tidewatch/pipeline/rules/news.py`
- Test: `tests/test_rules_news.py`

- [ ] **Step 1: 写失败的测试 `tests/test_rules_news.py`**

```python
from datetime import datetime, timezone

from tidewatch.config import KeywordRule
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
    assert not match_rule("任意文本", KeywordRule(id="empty"))  # all 与 any 都为空时不命中


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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_rules_news.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.pipeline.rules'`

- [ ] **Step 3: 实现 `tidewatch/pipeline/rules/keywords.py`**

```python
"""关键词组合匹配：all（全部包含）+ any（任一包含）+ none（排除）

文本与关键词都先做 NFKC（全角转半角）并转小写；纯英文/数字关键词要求前后不紧挨字母或数字
（避免 SEC 命中 consecutive），中文关键词按子串匹配。
"""

from __future__ import annotations

import re
import unicodedata

from tidewatch.config import KeywordRule


def _normalize(text: str | None) -> str:
    return unicodedata.normalize("NFKC", text or "").lower()


def _contains(content: str, word: str) -> bool:
    """content 须已规范化"""
    needle = _normalize(word)
    if needle.isascii():
        return re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", content) is not None
    return needle in content


def text_contains(text: str | None, word: str) -> bool:
    """文本是否包含关键词（规范化后比较；纯英文/数字关键词按独立单词匹配）"""
    return _contains(_normalize(text), word)


def match_rule(text: str | None, rule: KeywordRule) -> bool:
    """判断文本是否命中规则；all 与 any 都为空的规则视为不命中"""
    content = _normalize(text)
    if not (rule.all or rule.any):
        return False
    if not all(_contains(content, word) for word in rule.all):
        return False
    if rule.any and not any(_contains(content, word) for word in rule.any):
        return False
    return not any(_contains(content, word) for word in rule.none)


def matched_ids(text: str | None, rules: list[KeywordRule]) -> list[str]:
    """返回命中的规则 ID 列表（按配置顺序）"""
    return [rule.id for rule in rules if match_rule(text, rule)]
```

- [ ] **Step 4: 实现 `tidewatch/pipeline/rules/engine.py`**

```python
"""规则引擎：按事件类别分派到对应规则集"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.models import Event, RuleResult

if TYPE_CHECKING:
    from tidewatch.config import Settings


class RuleContext:
    """规则可用的只读上下文：当前事务连接与处理时刻

    一期规则不需要历史数据；后续期次（ETF 连续流入、内部人买入统计等）在此类上增加只读查询方法。
    规则只能通过 conn 读取数据，不得写库（写入统一由 EventSink 负责）。
    """

    def __init__(self, conn: AsyncConnection | None, now: datetime) -> None:
        self.conn = conn
        self.now = now


class CategoryRules(Protocol):
    """某一类别的规则集"""

    async def evaluate(self, event: Event, ctx: RuleContext) -> RuleResult: ...


class RuleEngine:
    """按 event.category 找到规则集并分级；没有规则集的类别一律为 C"""

    def __init__(self, rules_by_category: dict[str, CategoryRules]) -> None:
        self._rules = rules_by_category

    async def evaluate(self, event: Event, ctx: RuleContext) -> RuleResult:
        rules = self._rules.get(event.category)
        if rules is None:
            return RuleResult("C", ["no_rules"])
        return await rules.evaluate(event, ctx)
```

- [ ] **Step 5: 实现 `tidewatch/pipeline/rules/news.py`**

```python
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
from tidewatch.pipeline.rules.keywords import matched_ids


class NewsRules:
    def __init__(self, cfg: NewsRulesCfg) -> None:
        self._cfg = cfg

    async def evaluate(self, event: Event, ctx: RuleContext) -> RuleResult:
        text = f"{event.title}\n{event.body}"
        important = bool(event.raw.get("important"))
        s_hits = matched_ids(text, self._cfg.s_keywords)
        a_hits = matched_ids(text, self._cfg.a_keywords)
        has_asset = bool(event.assets)
        lowered = text.lower()
        has_person = any(person.lower() in lowered for person in self._cfg.people)

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
```

- [ ] **Step 6: 运行测试**

Run: `pytest tests/test_rules_news.py -v`
Expected: `3 passed`

- [ ] **Step 7: 提交**

```bash
git add tidewatch/pipeline/rules tests/test_rules_news.py
git commit -m "feat: 规则引擎与金十快讯分级规则" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: 交易所公告分级规则与引擎装配

**Files:**
- Create: `tidewatch/pipeline/rules/exchange.py`
- Modify: `tidewatch/pipeline/rules/engine.py`（新增 `build_rule_engine`）
- Test: `tests/test_rules_exchange.py`

- [ ] **Step 1: 写失败的测试 `tests/test_rules_exchange.py`**

```python
from datetime import datetime, timezone

import pytest

from tidewatch.models import Event
from tidewatch.pipeline.rules.engine import RuleContext, build_rule_engine
from tidewatch.pipeline.rules.exchange import ExchangeRules

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
CTX = RuleContext(conn=None, now=NOW)


def _ann(exchange: str, kind: str, assets: list[str]) -> Event:
    return Event(
        source=f"{exchange}_ann",
        category="exchange",
        event_type=kind,
        occurred_at=NOW,
        title=f"{exchange} {kind}",
        dedupe_key=f"{exchange}:{kind}:{assets}",
        assets=assets,
        raw={"exchange": exchange},
    )


@pytest.mark.parametrize(
    ("exchange", "kind", "assets", "level"),
    [
        ("binance", "spot_listing", ["XYZ"], "S"),
        ("okx", "spot_listing", ["XYZ"], "A"),
        ("binance", "perp_listing", ["XYZ"], "A"),
        ("okx", "perp_listing", ["XYZ"], "B"),
        ("okx", "delisting", ["XRP"], "A"),
        ("binance", "delisting", ["ABC"], "B"),
        ("binance", "perp_delisting", ["XRP"], "B"),
        ("okx", "deposit_withdraw_suspend", ["ETH"], "A"),
        ("okx", "deposit_withdraw_suspend", ["LSK"], "C"),
        ("binance", "other", ["HYPE"], "C"),
    ],
)
async def test_exchange_levels(exchange, kind, assets, level):
    rules = ExchangeRules(watch_symbols={"BTC", "ETH", "XRP", "HYPE"})
    result = await rules.evaluate(_ann(exchange, kind, assets), CTX)
    assert result.level == level
    assert f"exchange.{exchange}.{kind}" in result.hits


async def test_exchange_name_falls_back_to_source():
    event = _ann("binance", "spot_listing", ["XYZ"])
    event.raw = {}  # 采集器漏写 raw.exchange 时按来源名推断
    result = await ExchangeRules(watch_symbols=set()).evaluate(event, CTX)
    assert result.level == "S"


async def test_build_rule_engine(settings):
    engine = build_rule_engine(settings)
    assert (await engine.evaluate(_ann("binance", "spot_listing", ["XYZ"]), CTX)).level == "S"
    news = Event(source="jin10", category="news", event_type="flash", occurred_at=NOW,
                 title="美国9月非农就业人口增加8.2万人", dedupe_key="jin10:x")
    assert (await engine.evaluate(news, CTX)).level == "A"
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_rules_exchange.py -v`
Expected: FAIL，`ImportError: cannot import name 'build_rule_engine' from 'tidewatch.pipeline.rules.engine'`

- [ ] **Step 3: 实现 `tidewatch/pipeline/rules/exchange.py`**

```python
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
        exchange = str(event.raw.get("exchange") or event.source.removesuffix("_ann"))
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
```

- [ ] **Step 4: 在 `tidewatch/pipeline/rules/engine.py` 末尾追加装配函数**

```python
def build_rule_engine(settings: "Settings") -> RuleEngine:
    """按配置装配一期规则集（news、exchange）"""
    from tidewatch.pipeline.rules.exchange import ExchangeRules
    from tidewatch.pipeline.rules.news import NewsRules

    return RuleEngine(
        {
            "news": NewsRules(settings.rules.news),
            "exchange": ExchangeRules(settings.watch_symbols),
        }
    )
```

（函数内导入是为了避免 `engine.py` 与 `news.py`、`exchange.py` 之间的循环导入。）

- [ ] **Step 5: 运行本任务与上一任务的测试**

Run: `pytest tests/test_rules_exchange.py tests/test_rules_news.py -v && pytest -q`
Expected: `15 passed`（本任务 12 个：参数化 10 个 + 兜底 1 个 + 装配 1 个；上一任务 3 个）；全量回归全部通过（0 failed）

- [ ] **Step 6: 提交**

```bash
git add tidewatch/pipeline/rules tests/test_rules_exchange.py
git commit -m "feat: 交易所公告分级规则与规则引擎装配" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 4: 事件流水线（落库、聚簇、推送决策）

### Task 10: 通用 SQL（事件插入、分级回写、游标）

**Files:**
- Create: `tidewatch/store/repo.py`
- Test: `tests/test_repo.py`

- [ ] **Step 1: 写失败的测试 `tests/test_repo.py`**

```python
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from tidewatch.models import Event
from tidewatch.store.repo import insert_event, load_cursor, save_cursor, save_grading
from tidewatch.store.tables import collector_state, events

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


def _event(key: str, body: str = "") -> Event:
    return Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=NOW - timedelta(minutes=1),
        title="标题",
        body=body,
        dedupe_key=key,
        assets=["BTC"],
        raw={"important": 1},
    )


async def test_insert_event_and_duplicate(engine):
    event = _event("jin10:1", body="长" * 2500)
    async with engine.begin() as conn:
        first = await insert_event(conn, event, NOW)
        second = await insert_event(conn, _event("jin10:1"), NOW)
        row = (await conn.execute(select(events).where(events.c.id == first))).mappings().one()
    assert first is not None and second is None
    assert event.id == first and event.collected_at == NOW
    assert len(row["body"]) == 2000
    assert row["assets"] == ["BTC"] and row["collected_at"] == NOW


async def test_save_grading(engine):
    event = _event("jin10:2")
    async with engine.begin() as conn:
        await insert_event(conn, event, NOW)
        event.rule_level, event.rule_hits, event.final_level, event.ai_status = "A", ["x"], "A", "skipped"
        await save_grading(conn, event)
        row = (await conn.execute(select(events).where(events.c.id == event.id))).mappings().one()
    assert (row["rule_level"], row["rule_hits"], row["final_level"], row["ai_status"]) == ("A", ["x"], "A", "skipped")


async def test_cursor_roundtrip(engine):
    async with engine.begin() as conn:
        assert await load_cursor(conn, "jin10") is None
        await save_cursor(conn, "jin10", {"last_id": "1"}, NOW, had_events=True)
        await save_cursor(conn, "jin10", None, NOW + timedelta(minutes=1), had_events=False)
        assert await load_cursor(conn, "jin10") == {"last_id": "1"}  # cursor 为 None 时不覆盖
        row = (await conn.execute(select(collector_state))).mappings().one()
    assert row["last_event_at"] == NOW
    assert row["updated_at"] == NOW + timedelta(minutes=1)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_repo.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.store.repo'`

- [ ] **Step 3: 实现 `tidewatch/store/repo.py`**

```python
"""通用 SQL：事件插入与分级回写、采集器游标"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.models import Event
from tidewatch.store.tables import collector_state, events

BODY_LIMIT = 2000


async def insert_event(conn: AsyncConnection, event: Event, now: datetime) -> int | None:
    """插入事件；dedupe_key 已存在时不做修改并返回 None

    插入成功时回填 event.id 与 event.collected_at。
    """
    stmt = (
        pg_insert(events)
        .values(
            source=event.source,
            category=event.category,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            ref_date=event.ref_date,
            collected_at=now,
            title=event.title,
            body=(event.body or "")[:BODY_LIMIT],
            url=event.url,
            assets=list(event.assets),
            amount_usd=event.amount_usd,
            dedupe_key=event.dedupe_key,
            raw=event.raw,
        )
        .on_conflict_do_nothing(index_elements=["dedupe_key"])
        .returning(events.c.id)
    )
    event_id = (await conn.execute(stmt)).scalar_one_or_none()
    if event_id is not None:
        event.id = event_id
        event.collected_at = now
    return event_id


async def save_grading(conn: AsyncConnection, event: Event) -> None:
    """回写分级结果与 AI 状态"""
    await conn.execute(
        update(events)
        .where(events.c.id == event.id)
        .values(
            rule_level=event.rule_level,
            rule_hits=list(event.rule_hits),
            final_level=event.final_level,
            ai_status=event.ai_status,
        )
    )


async def load_cursor(conn: AsyncConnection, name: str) -> dict[str, Any] | None:
    """读取采集器游标，不存在时返回 None"""
    stmt = select(collector_state.c.cursor).where(collector_state.c.name == name)
    return (await conn.execute(stmt)).scalar_one_or_none()


async def save_cursor(
    conn: AsyncConnection,
    name: str,
    cursor: dict[str, Any] | None,
    now: datetime,
    *,
    had_events: bool,
) -> None:
    """写入采集器游标；cursor 为 None 时保留原游标；had_events 为真时更新 last_event_at"""
    values: dict[str, Any] = {"name": name, "updated_at": now}
    if cursor is not None:
        values["cursor"] = cursor
    if had_events:
        values["last_event_at"] = now
    stmt = pg_insert(collector_state).values(**values)
    set_ = {key: stmt.excluded[key] for key in values if key != "name"}
    await conn.execute(stmt.on_conflict_do_update(index_elements=["name"], set_=set_))
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_repo.py -v`
Expected: `3 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/store/repo.py tests/test_repo.py
git commit -m "feat: 事件插入去重与游标读写" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: 事件聚簇

**Files:**
- Create: `tidewatch/pipeline/cluster.py`
- Test: `tests/test_cluster.py`

规则（系统设计 6.4）：`news` 与窗口内 `news` 簇的首条事件比较标题相似度（≥ 阈值；双方资产都非空时还需有交集）；`news` 与 `exchange` 之间用显式规则（快讯标题含交易所名与上币/下架类关键词，且标题中出现公告涉及的任一币种代码）；两条不同的交易所公告不合并；窗口从簇的 `first_seen_at` 起算。

- [ ] **Step 1: 写失败的测试 `tests/test_cluster.py`**

```python
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from tidewatch.models import Event
from tidewatch.pipeline.cluster import Clusterer
from tidewatch.store.repo import insert_event, save_grading
from tidewatch.store.tables import event_clusters

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
NFP = "美国9月非农就业人口增加8.2万人，预期增加12万人，前值增加14.2万人"


async def _add(conn, clusterer, title, *, now=NOW, category="news", source="jin10", assets=None, level="B", key=None):
    event = Event(
        source=source,
        category=category,
        event_type="flash" if category == "news" else "spot_listing",
        occurred_at=now,
        title=title,
        dedupe_key=key or f"{source}:{title}:{now.isoformat()}",
        assets=assets or [],
        raw={"exchange": source.removesuffix("_ann")} if category == "exchange" else {},
    )
    await insert_event(conn, event, now)
    event.rule_level = event.final_level = level
    event.ai_status = "skipped"
    await save_grading(conn, event)
    return await clusterer.assign(conn, event, now)


def _clusterer(settings) -> Clusterer:
    return Clusterer(settings.pipeline, settings.rules.exchange)


async def test_similar_news_join_same_cluster(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        first = await _add(conn, clusterer, NFP, level="B")
        second = await _add(conn, clusterer, f"【{NFP}】", now=NOW + timedelta(minutes=3), level="A", source="other")
    assert first.id == second.id
    assert second.member_count == 2 and second.source_count == 2
    assert second.max_level == "A"


async def test_window_expired_creates_new_cluster(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        first = await _add(conn, clusterer, NFP)
        later = await _add(conn, clusterer, NFP, now=NOW + timedelta(minutes=11))
    assert first.id != later.id


async def test_disjoint_assets_not_merged(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        a = await _add(conn, clusterer, "大涨5%创新高", assets=["BTC"])
        b = await _add(conn, clusterer, "大涨5%创新高！", assets=["ETH"])
    assert a.id != b.id


async def test_listing_news_joins_announcement_either_order(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        ann = await _add(conn, clusterer, "Binance Will List XYZ (XYZ)", category="exchange", source="binance_ann", assets=["XYZ"], level="S")
        news = await _add(conn, clusterer, "币安将上线XYZ", level="B")
        news2 = await _add(conn, clusterer, "欧易将上线ABC", now=NOW + timedelta(minutes=1))
        ann2 = await _add(conn, clusterer, "OKX to list ABC (ABC) for spot trading", now=NOW + timedelta(minutes=2), category="exchange", source="okx_ann", assets=["ABC"], level="A")
    assert news.id == ann.id and news.source_count == 2 and news.max_level == "S"
    assert ann2.id == news2.id


async def test_different_announcements_not_merged(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        a = await _add(conn, clusterer, "Binance Will List XYZ (XYZ)", category="exchange", source="binance_ann", assets=["XYZ"])
        b = await _add(conn, clusterer, "Binance Will List XYZW (XYZW)", category="exchange", source="binance_ann", assets=["XYZW"])
        count = len((await conn.execute(select(event_clusters.c.id))).all())
    assert a.id != b.id and count == 2


async def test_listing_news_without_symbol_not_merged(engine, settings):
    clusterer = _clusterer(settings)
    async with engine.begin() as conn:
        ann = await _add(conn, clusterer, "Binance Will List XYZ (XYZ)", category="exchange", source="binance_ann", assets=["XYZ"])
        news = await _add(conn, clusterer, "币安将上线新币")
    assert news.id != ann.id
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_cluster.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.pipeline.cluster'`

- [ ] **Step 3: 实现 `tidewatch/pipeline/cluster.py`**

```python
"""事件聚簇（系统设计 6.4）

- news ↔ news：窗口内与簇首条事件的标题相似度 ≥ 阈值；双方资产都非空时还需有交集
- news ↔ exchange：快讯标题同时含交易所名与上币/下架类关键词，且标题中出现公告涉及的任一币种代码
- exchange ↔ exchange：不合并（同一公告的重复已由 dedupe_key 去除）
- 其他类别：每个事件单独成簇
窗口从簇的 first_seen_at 起算；候选全部从数据库查询，重启无需恢复内存状态。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.config import ExchangeRulesCfg, PipelineCfg
from tidewatch.models import Event, max_level
from tidewatch.pipeline.normalize import contains_symbol, title_similarity
from tidewatch.pipeline.rules.keywords import text_contains
from tidewatch.store.tables import event_clusters, events

_MATCHABLE = ("news", "exchange")


@dataclass
class ClusterState:
    """聚簇后簇的最新状态"""

    id: int
    category: str
    max_level: str
    committed_level: str | None
    delivered_level: str | None
    member_count: int
    source_count: int


class Clusterer:
    def __init__(self, pipeline_cfg: PipelineCfg, exchange_cfg: ExchangeRulesCfg) -> None:
        self._window = timedelta(seconds=pipeline_cfg.cluster_window_s)
        self._threshold = pipeline_cfg.title_similarity
        self._exchange_names = list(exchange_cfg.exchange_names)
        self._listing_keywords = list(exchange_cfg.listing_keywords)

    async def assign(self, conn: AsyncConnection, event: Event, now: datetime) -> ClusterState:
        """把已落库并分级的事件归入已有簇或新建簇"""
        cluster_id = None
        if event.category in _MATCHABLE:
            cluster_id = await self._find(conn, event, now - self._window)
        if cluster_id is None:
            return await self.create_single(conn, event, now)
        return await self._join(conn, cluster_id, event, now)

    async def create_single(self, conn: AsyncConnection, event: Event, now: datetime) -> ClusterState:
        """为事件新建一个只含它自己的簇"""
        cluster_id = (
            await conn.execute(
                insert(event_clusters)
                .values(
                    category=event.category,
                    first_event_id=event.id,
                    first_seen_at=now,
                    last_seen_at=now,
                    max_level=event.final_level,
                )
                .returning(event_clusters.c.id)
            )
        ).scalar_one()
        await self._attach(conn, event, cluster_id)
        return await self._state(conn, cluster_id)

    async def _find(self, conn: AsyncConnection, event: Event, since: datetime) -> int | None:
        stmt = (
            select(event_clusters.c.id, event_clusters.c.category, events.c.title, events.c.assets)
            .select_from(event_clusters.join(events, events.c.id == event_clusters.c.first_event_id))
            .where(event_clusters.c.category.in_(_MATCHABLE), event_clusters.c.first_seen_at >= since)
            .order_by(event_clusters.c.first_seen_at.desc())
        )
        for row in (await conn.execute(stmt)).all():
            if self._matches(event, row.category, row.title, row.assets):
                return row.id
        return None

    def _matches(self, event: Event, category: str, title: str, assets: list[str]) -> bool:
        if event.category == "news" and category == "news":
            if event.assets and assets and not set(event.assets) & set(assets):
                return False
            return title_similarity(event.title, title) >= self._threshold
        if {event.category, category} == {"news", "exchange"}:
            if event.category == "news":
                news_title, symbols = event.title, assets
            else:
                news_title, symbols = title, event.assets
            return self._is_listing_news(news_title) and any(contains_symbol(news_title, sym) for sym in symbols)
        return False

    def _is_listing_news(self, title: str) -> bool:
        # 英文关键词按独立单词匹配（避免 list 命中 blacklist）
        return any(text_contains(title, name) for name in self._exchange_names) and any(
            text_contains(title, word) for word in self._listing_keywords
        )

    async def _join(self, conn: AsyncConnection, cluster_id: int, event: Event, now: datetime) -> ClusterState:
        await self._attach(conn, event, cluster_id)
        counts = (
            await conn.execute(
                select(func.count(), func.count(func.distinct(events.c.source))).where(events.c.cluster_id == cluster_id)
            )
        ).one()
        current = (
            await conn.execute(select(event_clusters.c.max_level).where(event_clusters.c.id == cluster_id))
        ).scalar_one()
        await conn.execute(
            update(event_clusters)
            .where(event_clusters.c.id == cluster_id)
            .values(
                last_seen_at=now,
                member_count=counts[0],
                source_count=counts[1],
                max_level=max_level(current, event.final_level),
            )
        )
        return await self._state(conn, cluster_id)

    @staticmethod
    async def _attach(conn: AsyncConnection, event: Event, cluster_id: int) -> None:
        await conn.execute(update(events).where(events.c.id == event.id).values(cluster_id=cluster_id))
        event.cluster_id = cluster_id

    @staticmethod
    async def _state(conn: AsyncConnection, cluster_id: int) -> ClusterState:
        row = (
            await conn.execute(
                select(
                    event_clusters.c.id,
                    event_clusters.c.category,
                    event_clusters.c.max_level,
                    event_clusters.c.committed_level,
                    event_clusters.c.delivered_level,
                    event_clusters.c.member_count,
                    event_clusters.c.source_count,
                ).where(event_clusters.c.id == cluster_id)
            )
        ).one()
        return ClusterState(**row._mapping)

```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_cluster.py -v`
Expected: `6 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/pipeline/cluster.py tests/test_cluster.py
git commit -m "feat: 事件聚簇（标题相似度与快讯-公告跨类匹配）" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 12: 推送决策

**Files:**
- Create: `tidewatch/pipeline/decision.py`
- Test: `tests/test_decision.py`

规则（系统设计 6.7）：设 `F` = 簇内新鲜事件（`now − occurred_at ≤ 该源 max_push_age`）的最高 `final_level`，`C = committed_level`。`F ∈ {S, A}` 且 `C` 为空 → 首次推送；`F = S` 且 `C = A` → 把该簇 `pending`/`failed` 的推送标为 `superseded`，若 `delivered_level = A` 则 `kind = upgrade`，否则按 `event` 样式；其他情况不推送。代表事件为等级 `F` 的新鲜事件中最早的一条；重试期限 = min(代表事件新鲜度截止, now + retry_max)。

- [ ] **Step 1: 写失败的测试 `tests/test_decision.py`**

```python
from datetime import datetime, timedelta, timezone

from sqlalchemy import insert, select, update

from tidewatch.pipeline.decision import PushDecider
from tidewatch.store.tables import event_clusters, events, push_log

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)
AGES = {"jin10": timedelta(minutes=30), "etf": None}


def _decider() -> PushDecider:
    return PushDecider(lambda source: AGES.get(source, timedelta(minutes=30)), retry_max=timedelta(hours=24))


async def _cluster(conn, *, committed=None, delivered=None) -> int:
    return (
        await conn.execute(
            insert(event_clusters)
            .values(category="news", first_seen_at=NOW, last_seen_at=NOW, max_level="C",
                    committed_level=committed, delivered_level=delivered)
            .returning(event_clusters.c.id)
        )
    ).scalar_one()


async def _member(conn, cluster_id, level, *, minutes_ago=1, source="jin10", key=None) -> int:
    occurred = NOW - timedelta(minutes=minutes_ago)
    return (
        await conn.execute(
            insert(events)
            .values(source=source, category="news", event_type="flash", occurred_at=occurred,
                    collected_at=NOW, title=f"{level} 级", dedupe_key=key or f"{cluster_id}:{level}:{minutes_ago}",
                    final_level=level, rule_level=level, cluster_id=cluster_id)
            .returning(events.c.id)
        )
    ).scalar_one()


async def test_first_push(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        eid = await _member(conn, cid, "A", minutes_ago=10)
        plan = await _decider().decide(conn, cid, NOW)
        row = (await conn.execute(select(push_log))).mappings().one()
        cluster = (await conn.execute(select(event_clusters))).mappings().one()
    assert plan.level == "A" and plan.kind == "event" and plan.event_id == eid and plan.superseded_ids == []
    assert row["status"] == "pending" and row["id"] == plan.push_id
    assert row["retry_until"] == NOW - timedelta(minutes=10) + timedelta(minutes=30)
    assert cluster["committed_level"] == "A" and cluster["rep_event_id"] == eid


async def test_b_first_then_s(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "B", minutes_ago=5)
        sid = await _member(conn, cid, "S", minutes_ago=1)
        plan = await _decider().decide(conn, cid, NOW)
    assert plan.level == "S" and plan.event_id == sid and plan.kind == "event"


async def test_upgrade_after_a_delivered(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn, committed="A", delivered="A")
        await _member(conn, cid, "A", minutes_ago=5)
        await _member(conn, cid, "S", minutes_ago=1)
        plan = await _decider().decide(conn, cid, NOW)
    assert plan.level == "S" and plan.kind == "upgrade" and plan.superseded_ids == []


async def test_pending_a_superseded_by_s(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        aid = await _member(conn, cid, "A", minutes_ago=5)
        first = await _decider().decide(conn, cid, NOW)
        await conn.execute(update(events).where(events.c.id == aid).values(final_level="S"))
        second = await _decider().decide(conn, cid, NOW)
        statuses = {r.id: r.status for r in (await conn.execute(select(push_log.c.id, push_log.c.status))).all()}
    assert second.kind == "event" and second.level == "S"
    assert second.superseded_ids == [first.push_id]
    assert statuses == {first.push_id: "superseded", second.push_id: "pending"}


async def test_same_level_not_repeated(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn, committed="A")
        await _member(conn, cid, "A")
        assert await _decider().decide(conn, cid, NOW) is None
        assert (await conn.execute(select(push_log))).first() is None


async def test_stale_event_not_pushed(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "S", minutes_ago=120)
        assert await _decider().decide(conn, cid, NOW) is None


async def test_b_only_not_pushed(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "B")
        assert await _decider().decide(conn, cid, NOW) is None


async def test_unlimited_freshness_uses_retry_max(engine):
    async with engine.begin() as conn:
        cid = await _cluster(conn)
        await _member(conn, cid, "A", minutes_ago=600, source="etf")
        plan = await _decider().decide(conn, cid, NOW)
        retry_until = (await conn.execute(select(push_log.c.retry_until))).scalar_one()
    assert plan is not None and retry_until == NOW + timedelta(hours=24)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_decision.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.pipeline.decision'`

- [ ] **Step 3: 实现 `tidewatch/pipeline/decision.py`**

```python
"""以簇为单位的推送决策（系统设计 6.7）"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.models import max_level
from tidewatch.store.tables import event_clusters, events, push_log

# 按事件来源返回新鲜度上限（None 表示不限）
FreshnessLookup = Callable[[str], timedelta | None]

_DEFAULT_MAX_AGE = timedelta(minutes=30)


def freshness_from_settings(settings) -> FreshnessLookup:
    """从采集器配置生成新鲜度查询；未配置的来源默认 30 分钟"""

    def lookup(source: str) -> timedelta | None:
        cfg = settings.collectors.get(source)
        if cfg is None:
            return _DEFAULT_MAX_AGE
        return None if cfg.max_push_age_s is None else timedelta(seconds=cfg.max_push_age_s)

    return lookup


@dataclass
class PushPlan:
    """一次已安排（写入 push_log）的推送"""

    push_id: int
    cluster_id: int
    event_id: int
    level: str
    kind: str  # event / upgrade
    superseded_ids: list[int] = field(default_factory=list)


class PushDecider:
    def __init__(self, freshness: FreshnessLookup, retry_max: timedelta) -> None:
        self._freshness = freshness
        self._retry_max = retry_max

    def is_fresh(self, source: str, occurred_at: datetime, now: datetime) -> bool:
        max_age = self._freshness(source)
        return max_age is None or now - occurred_at <= max_age

    async def decide(self, conn: AsyncConnection, cluster_id: int, now: datetime) -> PushPlan | None:
        """根据簇当前状态决定是否安排推送；安排时写入 push_log 并更新 committed_level"""
        cluster = (
            await conn.execute(
                select(event_clusters.c.committed_level, event_clusters.c.delivered_level).where(
                    event_clusters.c.id == cluster_id
                )
            )
        ).one()
        members = (
            await conn.execute(
                select(events.c.id, events.c.source, events.c.occurred_at, events.c.final_level)
                .where(events.c.cluster_id == cluster_id)
                .order_by(events.c.occurred_at, events.c.id)
            )
        ).all()
        fresh = [m for m in members if self.is_fresh(m.source, m.occurred_at, now)]
        top = max_level(*(m.final_level for m in fresh))
        if top not in ("S", "A"):
            return None

        superseded: list[int] = []
        if cluster.committed_level is None:
            kind = "event"
        elif top == "S" and cluster.committed_level == "A":
            result = await conn.execute(
                update(push_log)
                .where(push_log.c.cluster_id == cluster_id, push_log.c.status.in_(("pending", "failed")))
                .values(status="superseded")
                .returning(push_log.c.id)
            )
            superseded = list(result.scalars().all())
            kind = "upgrade" if cluster.delivered_level == "A" else "event"
        else:
            return None

        rep = next(m for m in fresh if m.final_level == top)
        retry_until = now + self._retry_max
        max_age = self._freshness(rep.source)
        if max_age is not None:
            retry_until = min(retry_until, rep.occurred_at + max_age)
        push_id = (
            await conn.execute(
                insert(push_log)
                .values(
                    cluster_id=cluster_id,
                    event_id=rep.id,
                    level=top,
                    kind=kind,
                    status="pending",
                    retry_until=retry_until,
                    created_at=now,
                )
                .returning(push_log.c.id)
            )
        ).scalar_one()
        await conn.execute(
            update(event_clusters)
            .where(event_clusters.c.id == cluster_id)
            .values(committed_level=top, rep_event_id=rep.id)
        )
        return PushPlan(push_id, cluster_id, rep.id, top, kind, superseded)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_decision.py -v`
Expected: `8 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/pipeline/decision.py tests/test_decision.py
git commit -m "feat: 以簇为单位的推送决策" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 5: EventSink（串行入口与推送结果回写）

### Task 13: EventSink 串行入口与入库事务

**Files:**
- Create: `tidewatch/pipeline/sink.py`
- Test: `tests/test_sink.py`

`EventSink` 内部是一个 `asyncio.Queue` + 单个工作协程；`ingest` 把命令放入队列并等待结果（事务提交后才返回）。每批采集结果在一个事务内完成：插入去重 → 分级 → 聚簇 → 推送决策 → AI 标记 → 写游标；每条事件的分级/聚簇/决策包在 `begin_nested()` 保存点中，出错时回滚到保存点并降级为 C 级单事件簇。提交后调用 `on_ingest` 注册的监听器（推送调度器、SSE 广播将在后续任务注册）。

- [ ] **Step 1: 写失败的测试 `tests/test_sink.py`**

```python
from datetime import timedelta

import pytest
from sqlalchemy import select, text

from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.rules.engine import build_rule_engine
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.repo import load_cursor
from tidewatch.store.tables import events, push_log

NFP = "美国9月非农就业人口增加8.2万人"


def _flash(clock, key: str, title: str, *, important: int = 1, minutes_ago: int = 1) -> Event:
    return Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=clock.now - timedelta(minutes=minutes_ago),
        title=title,
        dedupe_key=key,
        raw={"important": important},
    )


@pytest.fixture
async def sink(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    yield event_sink
    await event_sink.stop()


async def test_ingest_grades_clusters_and_plans(sink, engine, clock):
    received = []
    sink.on_ingest(received.append)
    batch = FetchResult([_flash(clock, "jin10:1", NFP), _flash(clock, "jin10:2", "港股低开高走，恒指翻红")], {"last_id": "2"})
    ids = await sink.ingest(batch, collector="jin10")
    async with engine.connect() as conn:
        rows = {r.dedupe_key: r for r in (await conn.execute(select(events))).all()}
        pushes = (await conn.execute(select(push_log))).all()
        cursor = await load_cursor(conn, "jin10")
    assert len(ids) == 2
    assert rows["jin10:1"].final_level == "S" and rows["jin10:1"].ai_status == "skipped"
    assert rows["jin10:2"].final_level == "B" and rows["jin10:2"].cluster_id is not None
    assert rows["jin10:1"].cluster_id != rows["jin10:2"].cluster_id
    assert len(pushes) == 1 and pushes[0].level == "S" and pushes[0].status == "pending"
    assert cursor == {"last_id": "2"}
    assert len(received) == 1 and received[0].event_ids == ids and received[0].plans[0].level == "S"


async def test_assets_merged_with_watchlist(sink, engine, clock):
    event = _flash(clock, "jin10:3", "比特币突破12万美元", important=0)
    event.assets = ["XYZ"]
    await sink.ingest(FetchResult([event], None), collector="jin10")
    async with engine.connect() as conn:
        row = (await conn.execute(select(events.c.assets, events.c.final_level))).one()
    assert row.assets == ["XYZ", "BTC"] and row.final_level == "B"


async def test_duplicate_ingest_is_ignored(sink, engine, clock):
    assert len(await sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], {"last_id": "1"}), collector="jin10")) == 1
    assert await sink.ingest(FetchResult([_flash(clock, "jin10:1", NFP)], {"last_id": "5"}), collector="jin10") == []
    async with engine.connect() as conn:
        assert len((await conn.execute(select(push_log))).all()) == 1
        assert await load_cursor(conn, "jin10") == {"last_id": "5"}


async def test_rule_error_degrades_single_event(engine, settings, clock):
    real = build_rule_engine(settings)

    class Boom:
        async def evaluate(self, event, ctx):
            if event.dedupe_key == "bad":
                raise RuntimeError("规则缺陷")
            return await real.evaluate(event, ctx)

    event_sink = create_event_sink(engine, settings, clock=clock, rules=Boom())
    await event_sink.start()
    try:
        batch = FetchResult([_flash(clock, "bad", NFP), _flash(clock, "good", "美国宣布对中国加征关税")], {"last_id": "9"})
        await event_sink.ingest(batch, collector="jin10")
    finally:
        await event_sink.stop()
    async with engine.connect() as conn:
        rows = {r.dedupe_key: r for r in (await conn.execute(select(events))).all()}
        pushes = (await conn.execute(select(push_log))).all()
        cursor = await load_cursor(conn, "jin10")
    assert rows["bad"].final_level == "C" and rows["bad"].rule_hits == ["error"] and rows["bad"].cluster_id is not None
    assert rows["good"].final_level == "S" and len(pushes) == 1
    assert cursor == {"last_id": "9"}


async def test_db_error_in_rule_rolls_back_to_savepoint(engine, settings, clock):
    """规则里发生数据库错误（会使事务进入中止状态）时，必须回滚到保存点，同批其余事件照常提交"""
    real = build_rule_engine(settings)

    class DbBoom:
        async def evaluate(self, event, ctx):
            if event.dedupe_key == "bad":
                await ctx.conn.execute(text("SELECT 1/0"))
            return await real.evaluate(event, ctx)

    event_sink = create_event_sink(engine, settings, clock=clock, rules=DbBoom())
    await event_sink.start()
    try:
        batch = FetchResult([_flash(clock, "bad", NFP), _flash(clock, "good", "美国宣布对中国加征关税")], {"last_id": "7"})
        await event_sink.ingest(batch, collector="jin10")
    finally:
        await event_sink.stop()
    async with engine.connect() as conn:
        rows = {r.dedupe_key: r for r in (await conn.execute(select(events))).all()}
        assert await load_cursor(conn, "jin10") == {"last_id": "7"}
    assert rows["bad"].final_level == "C" and rows["good"].final_level == "S"


async def test_commit_failure_keeps_cursor(sink, engine, clock):
    broken = _flash(clock, "jin10:x", "标题")
    broken.title = None  # 违反 NOT NULL，整批事务失败
    with pytest.raises(Exception):
        await sink.ingest(FetchResult([_flash(clock, "jin10:ok", "港股低开高走"), broken], {"last_id": "x"}), collector="jin10")
    async with engine.connect() as conn:
        assert (await conn.execute(select(events))).first() is None
        assert await load_cursor(conn, "jin10") is None
    # 工作协程仍然可用
    assert len(await sink.ingest(FetchResult([_flash(clock, "jin10:ok", "港股低开高走")], {"last_id": "y"}), collector="jin10")) == 1


async def test_ai_enabled_marks_pending(engine, settings, clock):
    settings.ai.enabled = True
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    try:
        batch = FetchResult([_flash(clock, "b", "港股低开高走，恒指翻红"), _flash(clock, "c", "印度银行提供新服务", important=0)], None)
        await event_sink.ingest(batch, collector="jin10")
    finally:
        await event_sink.stop()
    async with engine.connect() as conn:
        rows = {r.dedupe_key: r for r in (await conn.execute(select(events))).all()}
    assert rows["b"].ai_status == "pending" and rows["c"].ai_status == "skipped"


async def test_exchange_assets_from_title_only(sink, engine, clock):
    event = Event(source="okx_ann", category="exchange", event_type="delisting", occurred_at=clock.now,
                  title="OKX to delist ABC", body="相关交易对 ABC/BTC、ABC/ETH 将下架", dedupe_key="okx:1",
                  assets=["ABC"], raw={"exchange": "okx"})
    await sink.ingest(FetchResult([event], None), collector="okx_ann")
    async with engine.connect() as conn:
        row = (await conn.execute(select(events.c.assets, events.c.final_level))).one()
    assert row.assets == ["ABC"] and row.final_level == "B"


async def test_not_started_raises(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    with pytest.raises(RuntimeError):
        await event_sink.ingest(FetchResult([], None), collector="jin10")
    await event_sink.start()
    await event_sink.stop()
    with pytest.raises(RuntimeError):  # 停止后再提交也立即报错，不会挂起
        await event_sink.ingest(FetchResult([], None), collector="jin10")
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_sink.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.pipeline.sink'`

- [ ] **Step 3: 实现 `tidewatch/pipeline/sink.py`**

```python
"""EventSink：唯一的串行写入入口（系统设计 6.3）

所有写库命令（采集结果 ingest、推送结果 push_result）进入同一个队列，由单个工作协程按顺序执行，
保证聚簇与推送决策不会竞争。事务提交后才通知监听者（推送调度器、SSE 广播）。
监听器必须是同步、不阻塞的函数（例如只把数据放进 asyncio.Queue）。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.models import Event, FetchResult, level_ge
from tidewatch.pipeline.cluster import Clusterer
from tidewatch.pipeline.decision import PushDecider, PushPlan, freshness_from_settings
from tidewatch.pipeline.normalize import AssetMatcher
from tidewatch.pipeline.rules.engine import CategoryRules, RuleContext, build_rule_engine
from tidewatch.store.repo import insert_event, save_cursor, save_grading

logger = logging.getLogger(__name__)

# 推送状态：可变（未完成）的状态与视为已送达的状态
OPEN_STATUSES = ("pending", "failed")
DELIVERED_STATUSES = ("ok", "dry_run")


@dataclass
class IngestOutcome:
    """一批采集结果提交后的摘要"""

    collector: str
    event_ids: list[int] = field(default_factory=list)
    cluster_ids: list[int] = field(default_factory=list)
    plans: list[PushPlan] = field(default_factory=list)


IngestListener = Callable[[IngestOutcome], None]


class EventSink:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        rules: CategoryRules,
        clusterer: Clusterer,
        decider: PushDecider,
        matcher: AssetMatcher,
        ai_enabled: bool,
        clock: Clock = utcnow,
    ) -> None:
        self._engine = engine
        self._rules = rules
        self._clusterer = clusterer
        self._decider = decider
        self._matcher = matcher
        self._ai_enabled = ai_enabled
        self._clock = clock
        self._queue: asyncio.Queue[tuple[Any, tuple, asyncio.Future] | None] = asyncio.Queue()
        self._worker: asyncio.Task | None = None
        self._closing = False
        self._ingest_listeners: list[IngestListener] = []

    # ---------- 生命周期与监听 ----------

    def on_ingest(self, listener: IngestListener) -> None:
        self._ingest_listeners.append(listener)

    async def start(self) -> None:
        if self._worker is None:
            self._closing = False
            self._worker = asyncio.create_task(self._run(), name="event-sink")

    async def stop(self) -> None:
        """处理完队列中已有的命令后停止；停止过程中新提交的命令立即报错"""
        if self._worker is None:
            return
        self._closing = True
        await self._queue.put(None)
        await self._worker
        self._worker = None

    # ---------- 对外命令 ----------

    async def ingest(self, result: FetchResult, *, collector: str) -> list[int]:
        """提交一批采集结果；事务提交后返回新插入的事件 id（重复事件不计入）"""
        return await self._submit(self._do_ingest, result, collector)

    async def _submit(self, func, *args):
        if self._worker is None or self._closing:
            raise RuntimeError("EventSink 尚未启动或正在停止")
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._queue.put((func, args, future))
        return await future

    async def _run(self) -> None:
        try:
            while True:
                item = await self._queue.get()
                if item is None:
                    return
                func, args, future = item
                try:
                    result = await func(*args)
                except Exception as exc:  # 异常交给调用方处理，工作协程继续运行
                    if future.done():
                        logger.exception("命令执行失败（调用方已取消）")
                    else:
                        future.set_exception(exc)
                else:
                    if not future.done():
                        future.set_result(result)
        finally:
            # 退出（含被取消）时，让仍在队列中的命令立即失败，避免调用方永久等待
            while not self._queue.empty():
                item = self._queue.get_nowait()
                if item is not None and not item[2].done():
                    item[2].set_exception(RuntimeError("EventSink 已停止"))

    # ---------- ingest ----------

    async def _do_ingest(self, result: FetchResult, collector: str) -> list[int]:
        now = self._clock()
        for event in result.events:
            self._prepare(event)
        outcome = IngestOutcome(collector=collector)
        async with self._engine.begin() as conn:
            ctx = RuleContext(conn, now)
            for event in result.events:
                if await insert_event(conn, event, now) is None:
                    continue
                outcome.event_ids.append(event.id)
                plan = await self._process(conn, event, ctx, now)
                outcome.cluster_ids.append(event.cluster_id)
                if plan is not None:
                    outcome.plans.append(plan)
            if result.cursor is not None or outcome.event_ids:
                await save_cursor(conn, collector, result.cursor, now, had_events=bool(outcome.event_ids))
        if outcome.event_ids:  # 大多数轮询批次没有新事件，不必通知
            self._emit(self._ingest_listeners, outcome)
        return outcome.event_ids

    def _prepare(self, event: Event) -> None:
        """合并采集器给出的资产与关注资产识别结果（保持顺序、去重）

        交易所公告只识别标题：正文常列出 xxx/BTC、xxx/USDT 等交易对，会误把关注资产算进来。
        """
        text = event.title if event.category == "exchange" else f"{event.title}\n{event.body}"
        found = self._matcher.find(text)
        event.assets = list(dict.fromkeys([*event.assets, *found]))

    async def _process(self, conn: AsyncConnection, event: Event, ctx: RuleContext, now) -> PushPlan | None:
        try:
            async with conn.begin_nested():
                rule = await self._rules.evaluate(event, ctx)
                event.rule_level, event.rule_hits, event.final_level = rule.level, list(rule.hits), rule.level
                event.ai_status = "pending" if self._ai_enabled and level_ge(rule.level, "B") else "skipped"
                await save_grading(conn, event)
                cluster = await self._clusterer.assign(conn, event, now)
                return await self._decider.decide(conn, cluster.id, now)
        except Exception:
            logger.exception("事件处理失败，已降级为 C 级：%s", event.dedupe_key)
            event.rule_level = event.final_level = "C"
            event.rule_hits = ["error"]
            event.ai_status = "skipped"
            event.cluster_id = None
            await save_grading(conn, event)
            await self._clusterer.create_single(conn, event, now)
            return None

    @staticmethod
    def _emit(listeners: list[Callable[[Any], None]], payload: Any) -> None:
        for listener in listeners:
            try:
                listener(payload)
            except Exception:
                logger.exception("监听器处理失败")


def create_event_sink(
    engine: AsyncEngine,
    settings,
    *,
    clock: Clock = utcnow,
    rules: CategoryRules | None = None,
) -> EventSink:
    """按配置装配 EventSink；rules 可在测试中替换"""
    return EventSink(
        engine,
        rules=rules or build_rule_engine(settings),
        clusterer=Clusterer(settings.pipeline, settings.rules.exchange),
        decider=PushDecider(freshness_from_settings(settings), timedelta(seconds=settings.dispatch.retry_max_s)),
        matcher=AssetMatcher(settings.watchlist.assets),
        ai_enabled=settings.ai.enabled,
        clock=clock,
    )
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_sink.py -v`
Expected: `9 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/pipeline/sink.py tests/test_sink.py
git commit -m "feat: EventSink 串行入口与事务化入库流程" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 14: 推送结果回写

**Files:**
- Modify: `tidewatch/pipeline/sink.py`
- Create: `tidewatch/store/repo.py` 中新增 `record_push_result`（SQL 放在 repo 层，sink 只负责编排）
- Test: `tests/test_sink_push_result.py`

规则（规范 notification「推送状态机」「取代处理」）：只有 `pending` / `failed` 记录会改变状态并累加 `attempts`；`ok` / `dry_run` 视为已送达，更新簇的 `delivered_level`（取较高者）；已处于终态（`superseded`、`ok`、`dry_run`、`expired`）的记录状态不变、`attempts` 不变，但若本次为已送达，仍更新簇的 `delivered_level`；`expired` 只改状态、不计为一次发送尝试。只接受 `ok`、`failed`、`dry_run`、`expired` 四种结果。

- [ ] **Step 1: 写失败的测试 `tests/test_sink_push_result.py`**

```python
from datetime import timedelta

import pytest
from sqlalchemy import select, update

from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.tables import event_clusters, push_log


@pytest.fixture
async def sink(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    yield event_sink
    await event_sink.stop()


async def _one_push(sink, engine, clock) -> int:
    event = Event(
        source="jin10",
        category="news",
        event_type="flash",
        occurred_at=clock.now - timedelta(minutes=1),
        title="美国9月非农就业人口增加8.2万人",
        dedupe_key="jin10:1",
        raw={"important": 1},
    )
    await sink.ingest(FetchResult([event], None), collector="jin10")
    async with engine.connect() as conn:
        return (await conn.execute(select(push_log.c.id))).scalar_one()


async def test_ok_updates_delivered_level(sink, engine, clock):
    seen = []
    sink.on_push_result(seen.append)
    push_id = await _one_push(sink, engine, clock)
    assert await sink.push_result(push_id, "ok", response="code=0") == "ok"
    async with engine.connect() as conn:
        row = (await conn.execute(select(push_log))).mappings().one()
        delivered = (await conn.execute(select(event_clusters.c.delivered_level))).scalar_one()
    assert row["status"] == "ok" and row["attempts"] == 1 and row["sent_at"] == clock.now
    assert row["response"] == "code=0" and delivered == "S"
    assert seen[0].status == "ok" and seen[0].push_id == push_id


async def test_superseded_keeps_status_but_records_delivery(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    async with engine.begin() as conn:
        await conn.execute(update(push_log).values(status="superseded"))
    assert await sink.push_result(push_id, "ok") == "superseded"
    async with engine.connect() as conn:
        delivered = (await conn.execute(select(event_clusters.c.delivered_level))).scalar_one()
    assert delivered == "S"


async def test_failed_then_dry_run_with_batch(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    assert await sink.push_result(push_id, "failed", response="超时") == "failed"
    assert await sink.push_result(push_id, "dry_run", batch_id="b1") == "dry_run"
    async with engine.connect() as conn:
        row = (await conn.execute(select(push_log))).mappings().one()
    assert row["attempts"] == 2 and row["batch_id"] == "b1" and row["status"] == "dry_run"


async def test_terminal_status_not_overwritten(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    await sink.push_result(push_id, "ok")
    assert await sink.push_result(push_id, "failed") == "ok"
    async with engine.connect() as conn:
        assert (await conn.execute(select(push_log.c.attempts))).scalar_one() == 1


async def test_expired_does_not_count_attempt(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    assert await sink.push_result(push_id, "expired") == "expired"
    async with engine.connect() as conn:
        assert (await conn.execute(select(push_log.c.attempts))).scalar_one() == 0


async def test_invalid_status_rejected(sink, engine, clock):
    push_id = await _one_push(sink, engine, clock)
    with pytest.raises(ValueError):
        await sink.push_result(push_id, "superseded")


async def test_unknown_push_id(sink):
    assert await sink.push_result(999999, "ok") is None
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_sink_push_result.py -v`
Expected: FAIL，`AttributeError: 'EventSink' object has no attribute 'on_push_result'`

- [ ] **Step 3: 在 `tidewatch/store/repo.py` 末尾追加 `record_push_result`**

```python
from tidewatch.models import max_level  # 放到文件顶部的导入区
from tidewatch.store.tables import event_clusters, push_log  # 与已有的 tables 导入合并

PUSH_RESULT_STATUSES = ("ok", "failed", "dry_run", "expired")


async def record_push_result(
    conn: AsyncConnection,
    push_id: int,
    status: str,
    now: datetime,
    *,
    response: str | None = None,
    batch_id: str | None = None,
) -> tuple[str, int] | None:
    """回写一次推送结果，返回 (最终状态, 簇 id)；记录不存在时返回 None"""
    if status not in PUSH_RESULT_STATUSES:
        raise ValueError(f"非法的推送结果状态：{status}")
    row = (
        await conn.execute(
            select(push_log.c.status, push_log.c.cluster_id, push_log.c.level)
            .where(push_log.c.id == push_id)
            .with_for_update()
        )
    ).one_or_none()
    if row is None:
        return None
    delivered = status in ("ok", "dry_run")
    final_status = row.status
    if row.status in ("pending", "failed"):
        final_status = status
        values: dict[str, Any] = {"status": status}
        if status != "expired":
            values["attempts"] = push_log.c.attempts + 1
        if response is not None:
            values["response"] = response[:500]
        if batch_id is not None:
            values["batch_id"] = batch_id
        if delivered:
            values["sent_at"] = now
        await conn.execute(update(push_log).where(push_log.c.id == push_id).values(**values))
    if delivered:
        current = (
            await conn.execute(select(event_clusters.c.delivered_level).where(event_clusters.c.id == row.cluster_id))
        ).scalar_one()
        await conn.execute(
            update(event_clusters)
            .where(event_clusters.c.id == row.cluster_id)
            .values(delivered_level=max_level(current, row.level))
        )
    return final_status, row.cluster_id
```

- [ ] **Step 4: 在 `tidewatch/pipeline/sink.py` 中新增推送结果回写**

在 `IngestListener = ...` 之后加入：

```python
@dataclass
class PushOutcome:
    """一次推送结果回写后的摘要"""

    push_id: int
    cluster_id: int
    status: str  # 回写后的实际状态


PushListener = Callable[[PushOutcome], None]
```

在 `__init__` 末尾加入 `self._push_listeners: list[PushListener] = []`；在 `on_ingest` 之后加入：

```python
    def on_push_result(self, listener: PushListener) -> None:
        self._push_listeners.append(listener)
```

在 `ingest` 方法之后加入：

```python
    async def push_result(
        self, push_id: int, status: str, *, response: str | None = None, batch_id: str | None = None
    ) -> str | None:
        """回写推送结果（ok / failed / dry_run / expired），返回记录的最终状态；记录不存在时返回 None"""
        return await self._submit(self._do_push_result, push_id, status, response, batch_id)
```

在 `_process` 方法之后加入：

```python
    async def _do_push_result(
        self, push_id: int, status: str, response: str | None, batch_id: str | None
    ) -> str | None:
        async with self._engine.begin() as conn:
            recorded = await record_push_result(
                conn, push_id, status, self._clock(), response=response, batch_id=batch_id
            )
        if recorded is None:
            logger.warning("推送记录不存在：%s", push_id)
            return None
        final_status, cluster_id = recorded
        self._emit(self._push_listeners, PushOutcome(push_id, cluster_id, final_status))
        return final_status
```

并把导入改为 `from tidewatch.store.repo import insert_event, record_push_result, save_cursor, save_grading`。

- [ ] **Step 5: 运行测试**

Run: `pytest tests/test_sink_push_result.py tests/test_sink.py -v && pytest -q`
Expected: `16 passed`（本任务 7 个 + Task 13 的 9 个）；全量回归全部通过（0 failed）

- [ ] **Step 6: 提交**

```bash
git add tidewatch/pipeline/sink.py tidewatch/store/repo.py tests/test_sink_push_result.py
git commit -m "feat: 推送结果回写与推送状态机" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 6: 通知器（接口、演练、飞书）

### Task 15: 通知器接口、演练通知器与本地时间格式化

**Files:**
- Create: `tidewatch/notify/__init__.py`（空文件）
- Create: `tidewatch/notify/base.py`
- Create: `tidewatch/notify/dryrun.py`
- Modify: `tidewatch/clock.py`（新增 `format_local`）
- Create: `tests/fakes.py`（测试用假通知器，后续任务复用）
- Test: `tests/test_notify_base.py`

- [ ] **Step 1: 写测试辅助 `tests/fakes.py`**

```python
"""测试替身"""

from __future__ import annotations

from tidewatch.notify.base import EventCard, Notifier, SendResult


class FakeNotifier(Notifier):
    """记录所有调用；results 为预设的发送结果队列，用完后一律成功"""

    status_on_success = "ok"

    def __init__(self, results: list[SendResult] | None = None) -> None:
        self.results = list(results or [])
        self.events: list[tuple[EventCard, bool]] = []
        self.digests: list[list[EventCard]] = []
        self.health: list[tuple[str, bool, str]] = []

    def _next(self) -> SendResult:
        return self.results.pop(0) if self.results else SendResult(True, detail="code=0")

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        self.events.append((card, mention_all))
        return self._next()

    async def send_digest(self, cards: list[EventCard]) -> SendResult:
        self.digests.append(list(cards))
        return self._next()

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        self.health.append((source, healthy, detail))
        return self._next()
```

- [ ] **Step 2: 写失败的测试 `tests/test_notify_base.py`**

```python
from datetime import datetime, timezone

from tidewatch.clock import format_local
from tidewatch.notify.base import EventCard, source_name
from tidewatch.notify.dryrun import DryRunNotifier

WHEN = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


def _card() -> EventCard:
    return EventCard(push_id=1, cluster_id=1, level="S", title="标题", source="jin10", occurred_at=WHEN,
                     url=None, assets=["BTC"], body="", source_count=1, upgraded=False)


def test_format_local_and_source_name():
    assert format_local(WHEN) == "2026-09-30 10:00:00"
    assert format_local(WHEN, "%H:%M") == "10:00"
    assert source_name("jin10") == "金十快讯" and source_name("unknown") == "unknown"


async def test_dry_run_notifier_records_and_succeeds():
    notifier = DryRunNotifier()
    assert notifier.status_on_success == "dry_run"
    assert (await notifier.send_event(_card(), mention_all=True)).ok
    assert (await notifier.send_digest([_card()])).ok
    assert (await notifier.send_health("jin10", False, "连续失败")).ok
    assert [kind for kind, _ in notifier.sent] == ["event", "digest", "health"]
```

- [ ] **Step 3: 运行测试确认失败**

Run: `mkdir -p tidewatch/notify && touch tidewatch/notify/__init__.py && pytest tests/test_notify_base.py -v`
Expected: FAIL，`ImportError: cannot import name 'format_local' from 'tidewatch.clock'`

- [ ] **Step 4: 在 `tidewatch/clock.py` 末尾追加**

```python
def format_local(when: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """把带时区的时间格式化为上海时间字符串"""
    return when.astimezone(LOCAL_TZ).strftime(fmt)
```

- [ ] **Step 5: 实现 `tidewatch/notify/base.py`**

```python
"""推送通道接口（系统设计 9.1）"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

SOURCE_NAMES = {"jin10": "金十快讯", "binance_ann": "币安公告", "okx_ann": "OKX 公告"}


def source_name(source: str) -> str:
    """采集器名 → 中文显示名"""
    return SOURCE_NAMES.get(source, source)


@dataclass
class EventCard:
    """一条待推送事件的展示内容"""

    push_id: int
    cluster_id: int
    level: str
    title: str
    source: str
    occurred_at: datetime
    url: str | None
    assets: list[str]
    body: str
    source_count: int
    upgraded: bool


@dataclass
class SendResult:
    ok: bool
    retryable: bool = False
    detail: str = ""


class Notifier(ABC):
    """推送通道；status_on_success 为发送成功时写入 push_log 的状态"""

    status_on_success: str = "ok"

    @abstractmethod
    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult: ...

    @abstractmethod
    async def send_digest(self, cards: list[EventCard]) -> SendResult: ...

    @abstractmethod
    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult: ...

    async def aclose(self) -> None:
        """释放连接等资源（默认无需处理）"""
```

- [ ] **Step 6: 实现 `tidewatch/notify/dryrun.py`**

```python
"""演练通知器：不发送任何消息，只写日志（--dry-run 使用）"""

from __future__ import annotations

import logging
from typing import Any

from tidewatch.notify.base import EventCard, Notifier, SendResult

logger = logging.getLogger(__name__)


class DryRunNotifier(Notifier):
    status_on_success = "dry_run"

    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        logger.info("【演练】%s 级推送：%s", card.level, card.title)
        self.sent.append(("event", card))
        return SendResult(True, detail="dry_run")

    async def send_digest(self, cards: list[EventCard]) -> SendResult:
        logger.info("【演练】A 级汇总 %d 条", len(cards))
        self.sent.append(("digest", cards))
        return SendResult(True, detail="dry_run")

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        logger.info("【演练】健康通知 %s：%s", source, detail)
        self.sent.append(("health", (source, healthy, detail)))
        return SendResult(True, detail="dry_run")
```

- [ ] **Step 7: 运行测试**

Run: `pytest tests/test_notify_base.py -v`
Expected: `2 passed`

- [ ] **Step 8: 提交**

```bash
git add tidewatch/clock.py tidewatch/notify tests/fakes.py tests/test_notify_base.py
git commit -m "feat: 通知器接口与演练通知器" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 16: 飞书通知器

**Files:**
- Create: `tidewatch/notify/feishu.py`
- Test: `tests/test_feishu.py`

飞书自定义机器人签名：`string_to_sign = f"{timestamp}\n{secret}"`，以其为 HMAC-SHA256 的密钥、对空消息计算摘要后 Base64。成功响应为 `{"code": 0, ...}`（旧版为 `{"StatusCode": 0, ...}`）；`code=11232` 为频率限制（可重试）；HTTP 429/5xx 与网络错误可重试；其他错误码（如签名错误 19021）不可重试。下列测试中的签名值已用该算法独立计算。

- [ ] **Step 1: 写失败的测试 `tests/test_feishu.py`**

```python
import json
from datetime import datetime, timezone

import httpx
import respx

from tidewatch.notify.base import EventCard
from tidewatch.notify.feishu import FeishuNotifier, sign

HOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/test"
WHEN = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


def _card(level="S", *, upgraded=False, url="https://flash.jin10.com/detail/1", source_count=1) -> EventCard:
    return EventCard(push_id=1, cluster_id=1, level=level, title="美国9月非农就业人口增加8.2万人", source="jin10",
                     occurred_at=WHEN, url=url, assets=["BTC"], body="正文内容", source_count=source_count,
                     upgraded=upgraded)


def _notifier(secret="test-secret") -> FeishuNotifier:
    return FeishuNotifier(HOOK, secret, client=httpx.AsyncClient(trust_env=False), clock=lambda: 1790700000)


def test_sign_known_vector():
    assert sign("1790700000", "test-secret") == "CX4RIQmjYTknzzq/eNcdTHe+fEBC6qxm8tpM81NnOLM="


@respx.mock
async def test_s_card_signed_with_mention():
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0, "msg": "success", "data": {}}))
    result = await _notifier().send_event(_card("S", source_count=3), mention_all=True)
    body = json.loads(route.calls.last.request.content)
    content = body["card"]["elements"][0]["text"]["content"]
    assert result.ok and result.detail == "code=0"
    assert body["msg_type"] == "interactive"
    assert body["timestamp"] == "1790700000" and body["sign"] == "CX4RIQmjYTknzzq/eNcdTHe+fEBC6qxm8tpM81NnOLM="
    assert body["card"]["header"]["template"] == "red"
    assert body["card"]["header"]["title"]["content"].startswith("【S】美国9月非农")
    assert "<at id=all></at>" in content and "2026-09-30 10:00:00" in content
    assert "金十快讯（另有 2 个来源）" in content and "BTC" in content and "正文内容" in content
    assert body["card"]["elements"][1]["actions"][0]["url"] == "https://flash.jin10.com/detail/1"


@respx.mock
async def test_upgraded_a_card_without_secret():
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"StatusCode": 0}))
    result = await _notifier(secret=None).send_event(_card("A", upgraded=True, url=None), mention_all=False)
    body = json.loads(route.calls.last.request.content)
    assert result.ok and "sign" not in body and "timestamp" not in body
    assert body["card"]["header"]["template"] == "orange"
    assert body["card"]["header"]["title"]["content"].startswith("【升级】【A】")
    assert "<at id=all></at>" not in body["card"]["elements"][0]["text"]["content"]
    assert len(body["card"]["elements"]) == 1  # 没有原文链接时不加按钮


@respx.mock
async def test_error_classification():
    notifier = _notifier()
    respx.post(HOOK).mock(side_effect=[
        httpx.Response(200, json={"code": 11232, "msg": "frequency limited"}),
        httpx.Response(200, json={"code": 19021, "msg": "sign match fail"}),
        httpx.Response(502, text="bad gateway"),
        httpx.ConnectError("连接失败"),
    ])
    limited = await notifier.send_event(_card(), mention_all=False)
    bad_sign = await notifier.send_event(_card(), mention_all=False)
    server = await notifier.send_event(_card(), mention_all=False)
    network = await notifier.send_event(_card(), mention_all=False)
    assert (limited.ok, limited.retryable) == (False, True) and "11232" in limited.detail
    assert (bad_sign.ok, bad_sign.retryable) == (False, False)
    assert (server.ok, server.retryable) == (False, True)
    assert (network.ok, network.retryable) == (False, True)


@respx.mock
async def test_digest_and_health_cards():
    route = respx.post(HOOK).mock(return_value=httpx.Response(200, json={"code": 0}))
    notifier = _notifier()
    await notifier.send_digest([_card("A"), _card("A", url=None)])
    digest = json.loads(route.calls.last.request.content)["card"]
    assert digest["header"]["title"]["content"] == "📋 A 级汇总（2 条）"
    assert digest["elements"][0]["text"]["content"].count("\n") == 1  # 两行
    await notifier.send_health("jin10", False, "连续失败 5 次")
    health = json.loads(route.calls.last.request.content)["card"]
    assert health["header"]["template"] == "red" and "金十快讯" in health["header"]["title"]["content"]
    await notifier.send_health("jin10", True, "恢复正常")
    recovered = json.loads(route.calls.last.request.content)["card"]
    assert recovered["header"]["template"] == "green"
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_feishu.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.notify.feishu'`

- [ ] **Step 3: 实现 `tidewatch/notify/feishu.py`**

```python
"""飞书群自定义机器人通知器"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any, Callable

import httpx

from tidewatch.clock import format_local
from tidewatch.notify.base import EventCard, Notifier, SendResult, source_name

# 频率限制等可重试的业务错误码
RETRYABLE_CODES = {11232}
_COLORS = {"S": "red", "A": "orange"}


def sign(timestamp: str, secret: str) -> str:
    """飞书签名：以 f"{timestamp}\\n{secret}" 为密钥对空消息做 HMAC-SHA256，再 Base64"""
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


class FeishuNotifier(Notifier):
    def __init__(
        self,
        webhook: str,
        secret: str | None,
        *,
        client: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._webhook = webhook
        self._secret = secret
        # 飞书在国内可直连，不使用环境代理
        self._client = client or httpx.AsyncClient(timeout=10, trust_env=False)
        self._clock = clock

    async def send_event(self, card: EventCard, *, mention_all: bool) -> SendResult:
        return await self._post(self._event_card(card, mention_all))

    async def send_digest(self, cards: list[EventCard]) -> SendResult:
        return await self._post(self._digest_card(cards))

    async def send_health(self, source: str, healthy: bool, detail: str) -> SendResult:
        return await self._post(self._health_card(source, healthy, detail))

    async def aclose(self) -> None:
        await self._client.aclose()

    # ---------- 卡片 ----------

    @staticmethod
    def _event_card(card: EventCard, mention_all: bool) -> dict[str, Any]:
        lines = ["<at id=all></at>"] if mention_all else []
        lines.append(f"**时间**：{format_local(card.occurred_at)}")
        extra = f"（另有 {card.source_count - 1} 个来源）" if card.source_count > 1 else ""
        lines.append(f"**来源**：{source_name(card.source)}{extra}")
        if card.assets:
            lines.append(f"**资产**：{'、'.join(card.assets)}")
        if card.body and card.body != card.title:
            lines.append(card.body[:300])
        elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}]
        if card.url:
            elements.append(
                {
                    "tag": "action",
                    "actions": [
                        {"tag": "button", "text": {"tag": "plain_text", "content": "查看原文"}, "type": "default", "url": card.url}
                    ],
                }
            )
        prefix = "【升级】" if card.upgraded else ""
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": _COLORS.get(card.level, "blue"),
                "title": {"tag": "plain_text", "content": f"{prefix}【{card.level}】{card.title[:80]}"},
            },
            "elements": elements,
        }

    @staticmethod
    def _digest_card(cards: list[EventCard]) -> dict[str, Any]:
        lines = []
        for card in cards:
            link = f" [原文]({card.url})" if card.url else ""
            lines.append(f"- {format_local(card.occurred_at, '%H:%M')} [{source_name(card.source)}] {card.title[:60]}{link}")
        return {
            "config": {"wide_screen_mode": True},
            "header": {"template": "orange", "title": {"tag": "plain_text", "content": f"📋 A 级汇总（{len(cards)} 条）"}},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}],
        }

    @staticmethod
    def _health_card(source: str, healthy: bool, detail: str) -> dict[str, Any]:
        title = f"✅ 采集器恢复：{source_name(source)}" if healthy else f"⚠️ 采集器异常：{source_name(source)}"
        return {
            "config": {"wide_screen_mode": True},
            "header": {"template": "green" if healthy else "red", "title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": detail[:500]}}],
        }

    # ---------- 发送 ----------

    def _payload(self, card: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {"msg_type": "interactive", "card": card}
        if self._secret:
            timestamp = str(int(self._clock()))
            payload["timestamp"] = timestamp
            payload["sign"] = sign(timestamp, self._secret)
        return payload

    async def _post(self, card: dict[str, Any]) -> SendResult:
        try:
            response = await self._client.post(self._webhook, json=self._payload(card))
        except httpx.HTTPError as exc:
            return SendResult(False, retryable=True, detail=f"网络错误：{exc}")
        if response.status_code == 429 or response.status_code >= 500:
            return SendResult(False, retryable=True, detail=f"HTTP {response.status_code}")
        try:
            data = response.json()
        except ValueError:
            return SendResult(False, retryable=False, detail=f"响应不是 JSON：HTTP {response.status_code}")
        code = data.get("code", data.get("StatusCode", -1))
        if code == 0:
            return SendResult(True, detail="code=0")
        return SendResult(False, retryable=code in RETRYABLE_CODES, detail=f"code={code} msg={data.get('msg')}")
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_feishu.py -v`
Expected: `5 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/notify/feishu.py tests/test_feishu.py
git commit -m "feat: 飞书自定义机器人通知器" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 7: 推送调度器与健康监控

### Task 17: 推送调度器

**Files:**
- Create: `tidewatch/notify/dispatcher.py`
- Test: `tests/test_dispatcher.py`

行为（系统设计 6.8、9.2）：S 级立即单独发送；A 级在 `a_window_s` 滑动窗口内最多单独发送 `a_window_max` 条，超出进入汇总缓冲，`flush_digest()`（后台每 `digest_interval_s` 调用一次）合并发送并写同一个 `batch_id`；发送失败先立即退避重试 `retry_immediate` 次（1、2、4… 秒），仍失败标为 `failed`；`retry_failed()`（后台每 `retry_interval_s` 调用一次）重发 `failed` 记录，过了 `retry_until` 的 `pending`/`failed` 记录标为 `expired`；启动时加载未过期的 `pending` 记录；被取代的记录从队列与缓冲中移除。所有状态回写经 `EventSink.push_result`。为便于测试，时钟、`sleep`、单调时钟均可注入，且提供 `process_pending()` 同步处理队列。

- [ ] **Step 1: 写失败的测试 `tests/test_dispatcher.py`**

```python
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import insert, select

from fakes import FakeNotifier
from tidewatch.notify.base import SendResult
from tidewatch.notify.dispatcher import Dispatcher
from tidewatch.notify.dryrun import DryRunNotifier
from tidewatch.pipeline.decision import PushPlan
from tidewatch.pipeline.sink import IngestOutcome, create_event_sink
from tidewatch.store.tables import event_clusters, events, push_log


class FakeMonotonic:
    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value


@pytest.fixture
async def sink(engine, settings, clock):
    event_sink = create_event_sink(engine, settings, clock=clock)
    await event_sink.start()
    yield event_sink
    await event_sink.stop()


async def _seed(engine, clock, level: str, n: int = 1, *, status: str = "pending", retry_minutes: int = 30) -> list[int]:
    """直接写入 n 个簇、事件与推送记录，返回 push_id 列表"""
    ids = []
    async with engine.begin() as conn:
        for i in range(n):
            cid = (await conn.execute(insert(event_clusters).values(
                category="news", first_seen_at=clock.now, last_seen_at=clock.now,
                max_level=level, committed_level=level).returning(event_clusters.c.id))).scalar_one()
            eid = (await conn.execute(insert(events).values(
                source="jin10", category="news", event_type="flash", occurred_at=clock.now, collected_at=clock.now,
                title=f"{level} 级测试 {i}", dedupe_key=f"t:{uuid.uuid4().hex}", final_level=level,
                cluster_id=cid).returning(events.c.id))).scalar_one()
            pid = (await conn.execute(insert(push_log).values(
                cluster_id=cid, event_id=eid, level=level, kind="event", status=status,
                retry_until=clock.now + timedelta(minutes=retry_minutes), created_at=clock.now,
            ).returning(push_log.c.id))).scalar_one()
            ids.append(pid)
    return ids


def _dispatcher(engine, sink, settings, clock, notifier, *, monotonic=None, sleeps=None) -> Dispatcher:
    async def fake_sleep(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    return Dispatcher(engine, sink, notifier, settings.dispatch, clock=clock, sleep=fake_sleep,
                      monotonic=monotonic or FakeMonotonic())


async def _statuses(engine) -> dict[int, tuple[str, str | None]]:
    async with engine.connect() as conn:
        rows = (await conn.execute(select(push_log.c.id, push_log.c.status, push_log.c.batch_id))).all()
    return {r.id: (r.status, r.batch_id) for r in rows}


async def test_s_sent_immediately_with_mention(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert notifier.events[0][1] is True and notifier.events[0][0].title == "S 级测试 0"
    assert (await _statuses(engine))[pid][0] == "ok"


async def test_a_window_overflow_goes_to_digest(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 13)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert len(notifier.events) == 10 and dispatcher.digest_size == 3
    [s_id] = await _seed(engine, clock, "S")  # S 级不受窗口限制
    dispatcher.enqueue([s_id])
    await dispatcher.process_pending()
    assert len(notifier.events) == 11
    await dispatcher.flush_digest()
    assert len(notifier.digests) == 1 and len(notifier.digests[0]) == 3
    statuses = await _statuses(engine)
    batch_ids = {statuses[i][1] for i in ids[10:]}
    assert all(statuses[i][0] == "ok" for i in ids) and len(batch_ids) == 1 and None not in batch_ids


async def test_window_slides(engine, sink, settings, clock):
    notifier, mono = FakeNotifier(), FakeMonotonic()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, monotonic=mono)
    dispatcher.enqueue(await _seed(engine, clock, "A", 11))
    await dispatcher.process_pending()
    assert dispatcher.digest_size == 1
    mono.value += settings.dispatch.a_window_s + 1
    dispatcher.enqueue(await _seed(engine, clock, "A", 1))
    await dispatcher.process_pending()
    assert len(notifier.events) == 11


async def test_immediate_retry_then_success(engine, sink, settings, clock):
    sleeps: list[float] = []
    notifier = FakeNotifier([SendResult(False, retryable=True, detail="code=11232"), SendResult(True, detail="code=0")])
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier, sleeps=sleeps)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert sleeps == [1.0] and (await _statuses(engine))[pid][0] == "ok"


async def test_non_retryable_marks_failed(engine, sink, settings, clock):
    notifier = FakeNotifier([SendResult(False, retryable=False, detail="code=19021")])
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and (await _statuses(engine))[pid][0] == "failed"


async def test_retry_failed_and_expire(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [alive] = await _seed(engine, clock, "A", status="failed", retry_minutes=30)
    [dead] = await _seed(engine, clock, "A", status="failed", retry_minutes=-1)
    [stuck] = await _seed(engine, clock, "S", status="pending", retry_minutes=-1)
    await dispatcher.retry_failed()
    statuses = await _statuses(engine)
    assert statuses[alive][0] == "ok" and statuses[dead][0] == "expired" and statuses[stuck][0] == "expired"
    assert len(notifier.events) == 1


async def test_retry_of_failed_digest_respects_window(engine, sink, settings, clock):
    notifier = FakeNotifier([SendResult(True)] * 10 + [SendResult(False, retryable=False, detail="code=19021")])
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 11)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    await dispatcher.flush_digest()  # 汇总发送失败 → 该条标为 failed
    assert (await _statuses(engine))[ids[10]][0] == "failed"
    await dispatcher.retry_failed()  # 窗口已满：回到汇总缓冲，而不是单独发送
    assert len(notifier.events) == 10 and dispatcher.digest_size == 1
    await dispatcher.flush_digest()
    assert (await _statuses(engine))[ids[10]][0] == "ok"


async def test_orphan_pending_recovered(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")  # 未入队的 pending（模拟异常遗漏）
    await dispatcher.retry_failed()
    assert notifier.events == []  # 刚创建的不算遗漏
    clock.advance(seconds=2 * settings.dispatch.digest_interval_s + 1)
    await dispatcher.retry_failed()
    assert len(notifier.events) == 1 and (await _statuses(engine))[pid][0] == "ok"


async def test_enqueue_dedupes(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    [pid] = await _seed(engine, clock, "S")
    dispatcher.enqueue([pid, pid])
    await dispatcher.process_pending()
    assert len(notifier.events) == 1


async def test_startup_loads_pending(engine, sink, settings, clock):
    notifier = FakeNotifier()
    [pid] = await _seed(engine, clock, "S")
    await _seed(engine, clock, "S", retry_minutes=-1)  # 已过期的不加载
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    await dispatcher.load_open()
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and (await _statuses(engine))[pid][0] == "ok"


async def test_superseded_removed_from_digest(engine, sink, settings, clock):
    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    ids = await _seed(engine, clock, "A", 11)
    dispatcher.enqueue(ids)
    await dispatcher.process_pending()
    assert dispatcher.digest_size == 1
    outcome = IngestOutcome(collector="jin10", plans=[PushPlan(999, 1, 1, "S", "event", superseded_ids=[ids[10]])])
    dispatcher._on_ingest(outcome)  # 模拟 EventSink 提交后的通知
    assert dispatcher.digest_size == 0
    await dispatcher.flush_digest()
    assert notifier.digests == []


async def test_dry_run_status(engine, sink, settings, clock):
    dispatcher = _dispatcher(engine, sink, settings, clock, DryRunNotifier())
    [pid] = await _seed(engine, clock, "A")
    dispatcher.enqueue([pid])
    await dispatcher.process_pending()
    assert (await _statuses(engine))[pid][0] == "dry_run"


async def test_attach_receives_plans_from_sink(engine, sink, settings, clock):
    from tidewatch.models import Event, FetchResult

    notifier = FakeNotifier()
    dispatcher = _dispatcher(engine, sink, settings, clock, notifier)
    dispatcher.attach()
    event = Event(source="jin10", category="news", event_type="flash", occurred_at=clock.now - timedelta(minutes=1),
                  title="美国9月非农就业人口增加8.2万人", dedupe_key="jin10:1", raw={"important": 1})
    await sink.ingest(FetchResult([event], None), collector="jin10")
    await dispatcher.process_pending()
    assert len(notifier.events) == 1 and notifier.events[0][0].level == "S"
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_dispatcher.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.notify.dispatcher'`

- [ ] **Step 3: 实现 `tidewatch/notify/dispatcher.py`**

```python
"""推送调度器（系统设计 6.8、9.2）

- S 级立即单独发送；A 级在滑动窗口内最多单独发送 N 条，超出进入汇总缓冲，定时合并发送
- 发送失败先立即退避重试，仍失败标记 failed，由后台定时重试，超过期限标记 expired
- 启动时加载未过期的 pending 记录并处理 failed 记录；被取代（superseded）的记录从队列与缓冲中移除
- 用 _tracked 记录已在队列、汇总缓冲或发送中的记录：入队去重，并定时找回因异常遗漏的 pending 记录
- 所有状态回写都经 EventSink.push_result 串行执行
- 调用顺序：先 attach()，再 start()，最后启动采集器
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.config import DispatchCfg
from tidewatch.notify.base import EventCard, Notifier, SendResult
from tidewatch.pipeline.sink import OPEN_STATUSES, EventSink, IngestOutcome
from tidewatch.store.tables import event_clusters, events, push_log

logger = logging.getLogger(__name__)

# 每条汇总消息最多包含的事件数（飞书请求体有大小限制）
DIGEST_MAX = 30


@dataclass
class _Pending:
    push_id: int
    status: str
    level: str
    retry_until: datetime
    card: EventCard


class Dispatcher:
    def __init__(
        self,
        engine: AsyncEngine,
        sink: EventSink,
        notifier: Notifier,
        cfg: DispatchCfg,
        *,
        clock: Clock = utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._engine = engine
        self._sink = sink
        self._notifier = notifier
        self._cfg = cfg
        self._clock = clock
        self._sleep = sleep
        self._monotonic = monotonic
        self._queue: asyncio.Queue[int] = asyncio.Queue()
        self._digest: list[int] = []
        self._cancelled: set[int] = set()
        self._tracked: set[int] = set()  # 已在队列、汇总缓冲或发送中的记录
        self._a_sent: deque[float] = deque()
        self._tasks: list[asyncio.Task] = []

    # ---------- 与 EventSink 对接 ----------

    def attach(self) -> None:
        """订阅 EventSink：事务提交后接收新安排的推送与被取代的推送"""
        self._sink.on_ingest(self._on_ingest)

    def _on_ingest(self, outcome: IngestOutcome) -> None:
        for plan in outcome.plans:
            self.cancel(plan.superseded_ids)
            self.enqueue([plan.push_id])

    def enqueue(self, push_ids: list[int]) -> None:
        """放入发送队列；已在跟踪中的记录不重复入队"""
        for push_id in push_ids:
            if push_id in self._tracked:
                continue
            self._tracked.add(push_id)
            self._queue.put_nowait(push_id)

    def cancel(self, push_ids: list[int]) -> None:
        """标记被取代的推送：不再发送，并从汇总缓冲移除"""
        self._cancelled.update(push_ids)
        removed = [i for i in self._digest if i in self._cancelled]
        self._digest = [i for i in self._digest if i not in self._cancelled]
        self._tracked.difference_update(removed)

    @property
    def digest_size(self) -> int:
        return len(self._digest)

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        """启动：加载未完成的推送，立即处理一次 failed 记录，再启动后台任务"""
        await self.load_open()
        await self.retry_failed()
        self._tasks = [
            asyncio.create_task(self._send_loop(), name="dispatch-send"),
            asyncio.create_task(self._periodic(self._cfg.digest_interval_s, self.flush_digest), name="dispatch-digest"),
            asyncio.create_task(self._periodic(self._cfg.retry_interval_s, self.retry_failed), name="dispatch-retry"),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def load_open(self) -> None:
        """启动恢复：把未过期的 pending 记录放回发送队列（failed 由定时重试处理）"""
        now = self._clock()
        async with self._engine.connect() as conn:
            ids = (
                await conn.execute(
                    select(push_log.c.id)
                    .where(push_log.c.status == "pending", push_log.c.retry_until > now)
                    .order_by(push_log.c.id)
                )
            ).scalars().all()
        self.enqueue(list(ids))
        if ids:
            logger.info("启动恢复：继续发送 %d 条未完成推送", len(ids))

    async def process_pending(self) -> None:
        """立即处理队列中所有待发送记录（测试与启动时使用）"""
        while not self._queue.empty():
            await self._handle(self._queue.get_nowait())

    async def _send_loop(self) -> None:
        while True:
            push_id = await self._queue.get()
            try:
                await self._handle(push_id)
            except Exception:
                logger.exception("处理推送失败：%s", push_id)

    async def _periodic(self, interval: float, func: Callable[[], Awaitable[None]]) -> None:
        while True:
            await self._sleep(interval)
            try:
                await func()
            except Exception:
                logger.exception("推送后台任务失败")

    # ---------- 发送 ----------

    async def _handle(self, push_id: int) -> None:
        """处理一条已跟踪的记录；除进入汇总缓冲外，处理结束即解除跟踪"""
        keep = False
        try:
            if push_id in self._cancelled:
                self._cancelled.discard(push_id)
                return
            pending = await self._load(push_id)
            if pending is None or pending.status not in OPEN_STATUSES:
                return
            if pending.retry_until <= self._clock():
                await self._sink.push_result(push_id, "expired")
                return
            if pending.level == "A" and not self._take_a_slot():
                if push_id not in self._digest:
                    self._digest.append(push_id)
                keep = True
                return
            await self._deliver(pending)
        finally:
            if not keep:
                self._tracked.discard(push_id)

    def _take_a_slot(self) -> bool:
        """A 级滑动窗口：窗口内已单独发送的数量未达上限时占用一个名额"""
        now = self._monotonic()
        while self._a_sent and now - self._a_sent[0] >= self._cfg.a_window_s:
            self._a_sent.popleft()
        if len(self._a_sent) < self._cfg.a_window_max:
            self._a_sent.append(now)
            return True
        return False

    async def _deliver(self, pending: _Pending) -> None:
        result = await self._send_with_retry(
            lambda: self._notifier.send_event(pending.card, mention_all=pending.level == "S")
        )
        status = self._notifier.status_on_success if result.ok else "failed"
        await self._sink.push_result(pending.push_id, status, response=result.detail)

    async def _send_with_retry(self, send: Callable[[], Awaitable[SendResult]]) -> SendResult:
        """立即退避重试：可重试的失败最多再试 retry_immediate 次，间隔 1、2、4… 秒"""
        delay = 1.0
        result = await send()
        for _ in range(self._cfg.retry_immediate):
            if result.ok or not result.retryable:
                break
            await self._sleep(delay)
            delay *= 2
            result = await send()
        if not result.ok:
            logger.warning("推送发送失败：%s", result.detail)
        return result

    async def flush_digest(self) -> None:
        """把汇总缓冲中的 A 级推送合并为一条消息发送（每条最多 DIGEST_MAX 个，其余留到下一轮）"""
        ids = [i for i in self._digest if i not in self._cancelled][:DIGEST_MAX]
        self._digest = [i for i in self._digest if i not in ids and i not in self._cancelled]
        try:
            now = self._clock()
            items = []
            for push_id in ids:
                pending = await self._load(push_id)
                if pending is not None and pending.status in OPEN_STATUSES and pending.retry_until > now:
                    items.append(pending)
            if not items:
                return
            batch_id = uuid.uuid4().hex[:12]
            result = await self._send_with_retry(lambda: self._notifier.send_digest([p.card for p in items]))
            status = self._notifier.status_on_success if result.ok else "failed"
            for pending in items:
                await self._sink.push_result(pending.push_id, status, response=result.detail, batch_id=batch_id)
        finally:
            self._tracked.difference_update(ids)  # 异常中断的记录仍是 pending，由 retry_failed 找回

    async def retry_failed(self) -> None:
        """定时维护：过期记录标为 expired；重发 failed 记录；找回因异常遗漏的 pending 记录

        重发同样经过 _handle，因此 A 级仍受滑动窗口限制（拿不到名额就回到汇总缓冲），不会刷屏。
        """
        now = self._clock()
        orphan_age = timedelta(seconds=2 * self._cfg.digest_interval_s)
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(push_log.c.id, push_log.c.status, push_log.c.retry_until, push_log.c.created_at)
                    .where(push_log.c.status.in_(OPEN_STATUSES))
                    .order_by(push_log.c.id)
                )
            ).all()
        for row in rows:
            if row.id in self._cancelled or row.id in self._tracked:
                continue
            if row.retry_until <= now:
                await self._sink.push_result(row.id, "expired")
            elif row.status == "failed" or now - row.created_at > orphan_age:
                self._tracked.add(row.id)
                await self._handle(row.id)

    async def _load(self, push_id: int) -> _Pending | None:
        stmt = (
            select(
                push_log.c.id,
                push_log.c.status,
                push_log.c.level,
                push_log.c.kind,
                push_log.c.retry_until,
                push_log.c.cluster_id,
                events.c.title,
                events.c.body,
                events.c.source,
                events.c.occurred_at,
                events.c.url,
                events.c.assets,
                event_clusters.c.source_count,
            )
            .select_from(
                push_log.join(events, events.c.id == push_log.c.event_id).join(
                    event_clusters, event_clusters.c.id == push_log.c.cluster_id
                )
            )
            .where(push_log.c.id == push_id)
        )
        async with self._engine.connect() as conn:
            row = (await conn.execute(stmt)).one_or_none()
        if row is None:
            return None
        card = EventCard(
            push_id=row.id,
            cluster_id=row.cluster_id,
            level=row.level,
            title=row.title,
            source=row.source,
            occurred_at=row.occurred_at,
            url=row.url,
            assets=list(row.assets),
            body=row.body,
            source_count=row.source_count,
            upgraded=row.kind == "upgrade",
        )
        return _Pending(row.id, row.status, row.level, row.retry_until, card)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_dispatcher.py -v`
Expected: `13 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/notify/dispatcher.py tests/test_dispatcher.py
git commit -m "feat: 推送调度器（防刷屏、汇总、重试与期限）" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 18: 采集器健康监控

**Files:**
- Create: `tidewatch/health.py`
- Test: `tests/test_health.py`

规则（系统设计 5.1、12；规范 news-sources「健康判定」、notification「健康告警通道」）：请求成功时记录 `last_success_at` 并清零失败计数；失败时累加 `consecutive_failures`；连续失败 ≥ 5 次或 `now − last_success_at > stale_after`（从未成功时以登记时刻起算）判为 `abnormal`；「没有新数据」不算失败。进入异常时通过 `send_health` 告警，同一采集器每 `health_alert_interval_s` 最多 1 条；若本次异常发过告警，恢复时发送恢复通知。状态持久化到 `collector_state`，启动时 `load()` 恢复状态与上次告警时间（避免崩溃重启绕过限流）；状态与失败次数变化通知监听者（供网站 SSE 使用）。

- [ ] **Step 1: 写失败的测试 `tests/test_health.py`**

```python
from sqlalchemy import select

from fakes import FakeNotifier
from tidewatch.health import HealthMonitor
from tidewatch.store.tables import collector_state


def _monitor(engine, settings, clock, notifier) -> HealthMonitor:
    monitor = HealthMonitor(engine, notifier, settings.dispatch, clock=clock)
    monitor.register("jin10", stale_after_s=300)
    return monitor


async def test_five_failures_then_one_alert(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    changes = []
    monitor.on_change(changes.append)
    for _ in range(6):
        await monitor.failure("jin10", "HTTP 503")
    assert monitor.status("jin10") == "abnormal"
    assert [(s, ok) for s, ok, _ in notifier.health] == [("jin10", False)]
    assert changes[-1]["name"] == "jin10" and changes[-1]["status"] == "abnormal"
    async with engine.connect() as conn:
        row = (await conn.execute(select(collector_state))).mappings().one()
    assert row["status"] == "abnormal" and row["consecutive_failures"] == 6 and row["last_error"] == "HTTP 503"


async def test_recovery_notice_and_alert_rate_limit(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await monitor.failure("jin10", "超时")
    await monitor.success("jin10")
    assert monitor.status("jin10") == "normal"
    clock.advance(minutes=10)
    for _ in range(5):  # 1 小时内再次异常：不再发异常告警
        await monitor.failure("jin10", "超时")
    assert [ok for _, ok, _ in notifier.health] == [False, True]
    clock.advance(hours=1)
    await monitor.success("jin10")  # 第二次异常没有告警，恢复时也不发恢复通知
    assert [ok for _, ok, _ in notifier.health] == [False, True]
    for _ in range(5):  # 超过 1 小时后再次异常：重新告警
        await monitor.failure("jin10", "超时")
    assert [ok for _, ok, _ in notifier.health] == [False, True, False]


async def test_load_restores_alert_time_after_restart(engine, settings, clock):
    notifier = FakeNotifier()
    first = _monitor(engine, settings, clock, notifier)
    for _ in range(5):
        await first.failure("jin10", "超时")
    assert len(notifier.health) == 1
    restarted = _monitor(engine, settings, clock, notifier)  # 模拟进程重启
    await restarted.load()
    assert restarted.status("jin10") == "abnormal"
    await restarted.success("jin10")
    for _ in range(5):
        await restarted.failure("jin10", "超时")
    assert [ok for _, ok, _ in notifier.health] == [False, True]  # 1 小时内不再告警


async def test_stale_detection(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    await monitor.success("jin10")
    clock.advance(seconds=299)
    await monitor.check_stale()
    assert monitor.status("jin10") == "normal"
    clock.advance(seconds=2)
    await monitor.check_stale()
    assert monitor.status("jin10") == "abnormal" and len(notifier.health) == 1


async def test_never_succeeded_uses_register_time(engine, settings, clock):
    notifier = FakeNotifier()
    monitor = _monitor(engine, settings, clock, notifier)
    clock.advance(seconds=301)
    await monitor.check_stale()
    assert monitor.status("jin10") == "abnormal"


async def test_mode_persisted(engine, settings, clock):
    monitor = _monitor(engine, settings, clock, FakeNotifier())
    await monitor.set_mode("jin10", "degraded")
    async with engine.connect() as conn:
        mode = (await conn.execute(select(collector_state.c.mode))).scalar_one()
    assert mode == "degraded"
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_health.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.health'`

- [ ] **Step 3: 实现 `tidewatch/health.py`**

```python
"""采集器健康监控（系统设计 5.1、12）"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import Clock, utcnow
from tidewatch.config import DispatchCfg
from tidewatch.notify.base import Notifier
from tidewatch.store.tables import collector_state

logger = logging.getLogger(__name__)

FAILURE_THRESHOLD = 5
CHECK_INTERVAL_S = 30

HealthListener = Callable[[dict[str, Any]], None]


@dataclass
class _State:
    stale_after_s: float
    registered_at: datetime
    status: str = "unknown"
    mode: str = "normal"
    last_success: datetime | None = None
    failures: int = 0
    last_alert: datetime | None = None
    last_error: str | None = None
    alerted: bool = False  # 本次异常是否已发出告警（决定恢复时是否发恢复通知）


class HealthMonitor:
    def __init__(
        self,
        engine: AsyncEngine,
        notifier: Notifier,
        cfg: DispatchCfg,
        *,
        clock: Clock = utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._engine = engine
        self._notifier = notifier
        self._alert_interval_s = cfg.health_alert_interval_s
        self._clock = clock
        self._sleep = sleep
        self._states: dict[str, _State] = {}
        self._listeners: list[HealthListener] = []

    def on_change(self, listener: HealthListener) -> None:
        self._listeners.append(listener)

    def register(self, name: str, stale_after_s: float) -> None:
        """登记一个需要监控的采集器（或子源）"""
        self._states[name] = _State(stale_after_s=stale_after_s, registered_at=self._clock())

    def status(self, name: str) -> str:
        return self._states[name].status

    async def load(self) -> None:
        """启动时从 collector_state 恢复已登记采集器的状态与上次告警时间，避免重启绕过告警限流"""
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(select(collector_state).where(collector_state.c.name.in_(list(self._states))))
            ).mappings().all()
        for row in rows:
            state = self._states[row["name"]]
            state.status = row["status"] or "unknown"
            state.mode = row["mode"] or "normal"
            state.last_alert = row["last_alert_at"]
            state.last_success = row["last_success_at"]
            state.failures = row["consecutive_failures"] or 0
            state.last_error = row["last_error"]
            state.alerted = state.status == "abnormal"

    async def set_mode(self, name: str, mode: str) -> None:
        """记录运行模式（normal / degraded），例如币安公告无 API Key 时为 degraded"""
        self._states[name].mode = mode
        await self._persist(name, mode=mode)
        self._emit(name)

    async def success(self, name: str) -> None:
        """一次成功的请求（或长连接心跳）"""
        state = self._states[name]
        now = self._clock()
        state.last_success = now
        state.failures = 0
        await self._persist(name, last_success_at=now, consecutive_failures=0)
        if state.status != "normal":
            await self._change(name, "normal", "恢复正常")

    async def failure(self, name: str, error: str) -> None:
        """一次失败的请求"""
        state = self._states[name]
        now = self._clock()
        state.failures += 1
        state.last_error = error[:500]
        await self._persist(name, last_error_at=now, last_error=state.last_error, consecutive_failures=state.failures)
        if state.failures >= FAILURE_THRESHOLD and state.status != "abnormal":
            await self._change(name, "abnormal", f"连续失败 {state.failures} 次：{state.last_error}")
        else:
            self._emit(name)  # 让状态页看到失败次数变化

    async def check_stale(self) -> None:
        """检查是否有采集器超过 stale_after 没有成功请求"""
        now = self._clock()
        for name, state in self._states.items():
            since = state.last_success or state.registered_at
            if state.status != "abnormal" and (now - since).total_seconds() > state.stale_after_s:
                await self._change(name, "abnormal", f"超过 {int(state.stale_after_s)} 秒没有成功请求")

    async def run(self) -> None:
        """后台循环：定期检查超时"""
        while True:
            await self._sleep(CHECK_INTERVAL_S)
            try:
                await self.check_stale()
            except Exception:
                logger.exception("健康检查失败")

    async def _change(self, name: str, status: str, detail: str) -> None:
        state = self._states[name]
        previous = state.status
        state.status = status
        now = self._clock()
        await self._persist(name, status=status)
        self._emit(name)
        if status == "abnormal":
            logger.warning("采集器异常：%s，%s", name, detail)
            state.alerted = False
            if state.last_alert is None or (now - state.last_alert).total_seconds() >= self._alert_interval_s:
                state.last_alert = now
                state.alerted = True
                await self._persist(name, last_alert_at=now)
                await self._notifier.send_health(name, False, detail)
        elif previous == "abnormal":
            logger.info("采集器恢复：%s", name)
            if state.alerted:  # 只有本次异常发过告警才发恢复通知，避免只收到「恢复」
                await self._notifier.send_health(name, True, detail)
            state.alerted = False

    def _emit(self, name: str) -> None:
        state = self._states[name]
        payload = {
            "name": name,
            "status": state.status,
            "mode": state.mode,
            "consecutive_failures": state.failures,
            "last_error": state.last_error,
            "last_success_at": state.last_success.isoformat() if state.last_success else None,
        }
        for listener in self._listeners:
            try:
                listener(payload)
            except Exception:
                logger.exception("健康状态监听器失败")

    async def _persist(self, name: str, **values: Any) -> None:
        values["updated_at"] = self._clock()
        stmt = pg_insert(collector_state).values(name=name, **values)
        stmt = stmt.on_conflict_do_update(index_elements=["name"], set_={k: stmt.excluded[k] for k in values})
        async with self._engine.begin() as conn:
            await conn.execute(stmt)
```

- [ ] **Step 4: 运行测试与全量回归**

Run: `pytest tests/test_health.py -v && pytest -q`
Expected: `6 passed`；全量回归全部通过（0 failed）

- [ ] **Step 5: 提交**

```bash
git add tidewatch/health.py tests/test_health.py
git commit -m "feat: 采集器健康监控与告警" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 8: 采集器基类与公告处理

### Task 19: 采集器基类与 HTTP 客户端

**Files:**
- Create: `tidewatch/collectors/__init__.py`（空文件）
- Create: `tidewatch/collectors/base.py`
- Create: `tidewatch/collectors/http.py`
- Modify: `tests/fakes.py`（新增 `FakeSink`、`FakeHealth`、`FakeSleep`）
- Test: `tests/test_collector_base.py`

基类约定（系统设计 5.1）：轮询型实现 `fetch(cursor)`；长连接型实现 `stream(cursor)`（心跳时产出 `FetchResult([], None)`）与 `gap_fill(cursor)`。基类负责：调度、令牌桶限流（子类在每次 HTTP 请求前 `await self.limiter.acquire()`）、指数退避（1 秒起 ×2，上限 300 秒，成功后重置）、`EventSink.ingest` 成功后才采用新游标、健康上报（请求成功 → `success`，异常 → `failure`）。

- [ ] **Step 1: 在 `tests/fakes.py` 末尾追加测试替身**

```python
import asyncio

from tidewatch.models import FetchResult


class FakeSink:
    """记录 ingest 调用；fail_on 为需要抛异常的调用序号（从 1 开始）"""

    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.calls: list[tuple[str, FetchResult]] = []
        self.fail_on = fail_on or set()

    async def ingest(self, result: FetchResult, *, collector: str) -> list[int]:
        self.calls.append((collector, result))
        if len(self.calls) in self.fail_on:
            raise RuntimeError("数据库暂时不可用")
        return list(range(len(result.events)))


class FakeHealth:
    def __init__(self) -> None:
        self.successes: list[str] = []
        self.failures: list[tuple[str, str]] = []

    async def success(self, name: str) -> None:
        self.successes.append(name)

    async def failure(self, name: str, error: str) -> None:
        self.failures.append((name, error))


class FakeSleep:
    """记录等待时长；第 stop_after 次调用时抛 CancelledError，用于结束无限循环"""

    def __init__(self, stop_after: int) -> None:
        self.delays: list[float] = []
        self.stop_after = stop_after

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        if len(self.delays) >= self.stop_after:
            raise asyncio.CancelledError
```

- [ ] **Step 2: 写失败的测试 `tests/test_collector_base.py`**

```python
import asyncio
from datetime import datetime, timezone

import pytest

from fakes import FakeHealth, FakeSink, FakeSleep
from tidewatch.collectors.base import Backoff, PollCollector, StreamCollector, TokenBucket
from tidewatch.collectors.http import make_client
from tidewatch.config import CollectorCfg
from tidewatch.models import Event, FetchResult

CFG = CollectorCfg(interval_s=20, stale_after_s=300, gap_fill_interval_s=600, rate_per_s=2)
NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


def _event(key: str) -> Event:
    return Event(source="demo", category="news", event_type="flash", occurred_at=NOW, title=key, dedupe_key=key)


def test_backoff_sequence():
    backoff = Backoff()
    assert [backoff.next() for _ in range(10)] == [1, 2, 4, 8, 16, 32, 64, 128, 256, 300]
    backoff.reset()
    assert backoff.next() == 1


async def test_token_bucket_waits_when_empty():
    clock = {"t": 0.0}
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["t"] += seconds

    bucket = TokenBucket(2, monotonic=lambda: clock["t"], sleep=fake_sleep)
    for _ in range(3):
        await bucket.acquire()
    assert sleeps == [pytest.approx(0.5)]


class DemoPoll(PollCollector):
    name = "demo"

    def __init__(self, **kwargs) -> None:
        super().__init__(CFG, **kwargs)
        self.seen: list[dict | None] = []

    async def load_cursor(self):
        return None

    async def fetch(self, cursor):
        self.seen.append(cursor)
        n = len(self.seen)
        return FetchResult([_event(f"e{n}")], {"n": n})


async def test_poll_keeps_old_cursor_when_ingest_fails():
    sink, health, sleep = FakeSink(fail_on={2}), FakeHealth(), FakeSleep(stop_after=3)
    collector = DemoPoll(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    assert collector.seen == [None, {"n": 1}, {"n": 1}]
    assert sleep.delays == [20, 1, 20]
    assert len(health.failures) == 1 and "数据库暂时不可用" in health.failures[0][1]


class BrokenHealth(FakeHealth):
    async def failure(self, name: str, error: str) -> None:
        raise RuntimeError("数据库断开，健康状态写不进去")


async def test_poll_survives_health_report_failure():
    sink, health, sleep = FakeSink(fail_on={1}), BrokenHealth(), FakeSleep(stop_after=2)
    collector = DemoPoll(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    assert collector.seen == [None, None]  # 第一轮失败后保持旧游标并继续
    assert sleep.delays == [1, 20]


class DemoStream(StreamCollector):
    name = "demo_ws"

    def __init__(self, **kwargs) -> None:
        super().__init__(CFG, **kwargs)
        self.gap_cursors: list[dict | None] = []

    async def load_cursor(self):
        return {"last_ms": 1}

    async def gap_fill(self, cursor):
        self.gap_cursors.append(cursor)
        return FetchResult([], {"last_ms": 5})

    async def stream(self, cursor):
        yield FetchResult([], None)  # 心跳
        yield FetchResult([_event("ws1")], {"last_ms": 9})

    def merge_cursor(self, old, new):
        if new is None:
            return old
        return {"last_ms": max((old or {}).get("last_ms", 0), new["last_ms"])}


async def test_stream_gap_fill_then_stream_then_reconnect():
    sink, health, sleep = FakeSink(), FakeHealth(), FakeSleep(stop_after=1)
    collector = DemoStream(sink=sink, health=health, client=None, engine=None, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    assert collector.gap_cursors == [{"last_ms": 1}]
    ingested = [result for _, result in sink.calls]
    assert [len(r.events) for r in ingested] == [0, 1]  # 补缺结果 + WS 事件；心跳不入库
    assert collector.cursor == {"last_ms": 9}
    assert len(health.successes) == 2  # 只在收到长连接消息时上报成功
    assert health.failures and "长连接" in health.failures[0][1]  # 连接结束后按失败处理并退避
    assert sleep.delays == [1]


def test_make_client_proxy_modes():
    direct = make_client("direct", "http://127.0.0.1:10808")
    proxied = make_client("global", "http://127.0.0.1:10808")
    no_proxy_configured = make_client("global", None)
    assert not direct._mounts and proxied._mounts and not no_proxy_configured._mounts
```

- [ ] **Step 3: 运行测试确认失败**

Run: `mkdir -p tidewatch/collectors && touch tidewatch/collectors/__init__.py && pytest tests/test_collector_base.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.collectors.base'`

- [ ] **Step 4: 实现 `tidewatch/collectors/http.py`**

```python
"""HTTP 客户端：按采集器的代理设置创建 httpx 客户端"""

from __future__ import annotations

import httpx

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)


def make_client(proxy_mode: str, proxy_url: str | None, *, timeout: float = 20.0) -> httpx.AsyncClient:
    """proxy_mode 为 global 且配置了代理地址时走代理，否则直连；不读取系统环境代理"""
    proxy = proxy_url if proxy_mode == "global" and proxy_url else None
    return httpx.AsyncClient(
        proxy=proxy,
        timeout=timeout,
        trust_env=False,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )
```

- [ ] **Step 5: 实现 `tidewatch/collectors/base.py`**

```python
"""采集器契约与基类（系统设计 5.1）"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Awaitable, Callable

from tidewatch.clock import Clock, utcnow
from tidewatch.config import CollectorCfg
from tidewatch.models import FetchResult
from tidewatch.store.repo import load_cursor

logger = logging.getLogger(__name__)

Sleep = Callable[[float], Awaitable[None]]


def describe(exc: BaseException) -> str:
    """异常的简短中文可读描述"""
    return f"{type(exc).__name__}：{exc}"[:300]


class TokenBucket:
    """令牌桶：每秒补充 rate 个令牌，容量 capacity；没有令牌时等待"""

    def __init__(
        self,
        rate_per_s: float,
        capacity: float | None = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._rate = rate_per_s
        self._capacity = capacity or max(1.0, rate_per_s)
        self._tokens = self._capacity
        self._monotonic = monotonic
        self._sleep = sleep
        self._last = monotonic()

    async def acquire(self) -> None:
        while True:
            now = self._monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
            self._last = now
            if self._tokens >= 1:
                self._tokens -= 1
                return
            await self._sleep((1 - self._tokens) / self._rate)


class Backoff:
    """指数退避：1 秒起，每次 ×2，上限 300 秒；成功后 reset"""

    def __init__(self, base: float = 1.0, factor: float = 2.0, cap: float = 300.0) -> None:
        self._base, self._factor, self._cap = base, factor, cap
        self._current = base

    def next(self) -> float:
        value = self._current
        self._current = min(self._current * self._factor, self._cap)
        return value

    def reset(self) -> None:
        self._current = self._base


class Collector(ABC):
    """所有采集器的基类；子类需设置 name 与 group"""

    name: str = ""
    group: str = "news"

    def __init__(
        self,
        cfg: CollectorCfg,
        *,
        sink: Any,
        health: Any,
        client: Any,
        engine: Any,
        clock: Clock = utcnow,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.cfg = cfg
        self.sink = sink
        self.health = health
        self.client = client
        self.engine = engine
        self.clock = clock
        self._sleep = sleep
        self.limiter = TokenBucket(cfg.rate_per_s, sleep=sleep)
        self.backoff = Backoff()

    async def load_cursor(self) -> dict[str, Any] | None:
        async with self.engine.connect() as conn:
            return await load_cursor(conn, self.name)

    async def _load_cursor_with_retry(self) -> dict[str, Any] | None:
        """读取游标；失败（如数据库暂时不可用）时退避重试，不让采集器退出"""
        while True:
            try:
                return await self.load_cursor()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 读取游标失败：%s", self.name, describe(exc))
                await self._sleep(self.backoff.next())

    async def _report_success(self) -> None:
        """上报健康状态；上报本身失败（如数据库断开）只记日志，不影响采集循环"""
        try:
            await self.health.success(self.name)
        except Exception:
            logger.exception("采集器 %s 健康状态上报失败", self.name)

    async def _report_failure(self, exc: BaseException) -> None:
        try:
            await self.health.failure(self.name, describe(exc))
        except Exception:
            logger.exception("采集器 %s 健康状态上报失败", self.name)

    @abstractmethod
    async def run(self) -> None:
        """运行直到被取消"""


class PollCollector(Collector):
    """轮询型采集器：子类实现 fetch(cursor)"""

    @abstractmethod
    async def fetch(self, cursor: dict[str, Any] | None) -> FetchResult: ...

    async def run(self) -> None:
        cursor = await self._load_cursor_with_retry()
        while True:
            try:
                result = await self.fetch(cursor)
                await self._report_success()
                await self.sink.ingest(result, collector=self.name)
                if result.cursor is not None:  # 提交成功后才采用新游标
                    cursor = result.cursor
                self.backoff.reset()
                delay = self.cfg.interval_s
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 本轮失败：%s", self.name, describe(exc))
                await self._report_failure(exc)
                delay = self.backoff.next()
            await self._sleep(delay)


class StreamCollector(Collector):
    """长连接型采集器：子类实现 stream(cursor) 与 gap_fill(cursor)

    每次（重新）连接前先 gap_fill 补缺，连接期间另有后台任务每 gap_fill_interval_s 补缺一次；
    长连接结束（无论正常关闭还是出错）都按失败处理并退避后重连。
    """

    def __init__(self, cfg: CollectorCfg, **kwargs: Any) -> None:
        super().__init__(cfg, **kwargs)
        self.cursor: dict[str, Any] | None = None

    @abstractmethod
    def stream(self, cursor: dict[str, Any] | None) -> AsyncIterator[FetchResult]: ...

    @abstractmethod
    async def gap_fill(self, cursor: dict[str, Any] | None) -> FetchResult: ...

    def merge_cursor(self, old: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any] | None:
        """合并游标；默认新游标覆盖旧游标"""
        return new if new is not None else old

    async def _commit(self, result: FetchResult) -> None:
        """先合并游标再入库，避免较旧的游标覆盖库中较新的游标"""
        merged = self.merge_cursor(self.cursor, result.cursor)
        await self.sink.ingest(FetchResult(result.events, merged, result.records), collector=self.name)
        self.cursor = merged

    async def _gap_fill_loop(self) -> None:
        interval = self.cfg.gap_fill_interval_s or 600
        while True:
            await self._sleep(interval)
            try:
                await self._commit(await self.gap_fill(self.cursor))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 定时补缺失败：%s", self.name, describe(exc))

    async def run(self) -> None:
        self.cursor = await self._load_cursor_with_retry()
        while True:
            filler: asyncio.Task | None = None
            try:
                await self._commit(await self.gap_fill(self.cursor))
                filler = asyncio.create_task(self._gap_fill_loop(), name=f"{self.name}-gap-fill")
                # aclosing：入库失败退出循环时立即关闭长连接
                async with contextlib.aclosing(self.stream(self.cursor)) as messages:
                    async for result in messages:
                        # 只在收到长连接消息时上报成功（补缺成功不代表长连接可用）
                        await self._report_success()
                        if result.events or result.cursor is not None:
                            await self._commit(result)
                        self.backoff.reset()
                raise ConnectionError("长连接已关闭")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("采集器 %s 连接中断：%s", self.name, describe(exc))
                await self._report_failure(exc)
                await self._sleep(self.backoff.next())
            finally:
                if filler is not None:
                    filler.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await filler
```

- [ ] **Step 6: 运行测试**

Run: `pytest tests/test_collector_base.py -v`
Expected: `6 passed`

- [ ] **Step 7: 提交**

```bash
git add tidewatch/collectors tests/fakes.py tests/test_collector_base.py
git commit -m "feat: 采集器基类（限流、退避、游标、健康上报）" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 20: 公告分类与币种提取

**Files:**
- Create: `tidewatch/collectors/announcements.py`
- Test: `tests/test_announcements.py`

分类先用交易所自己的栏目确定大类（OKX 的 `annType`，币安的栏目 ID：48 新币上线、161 下架），再用标题细分现货/合约；没有栏目信息时（币安 WS 其他栏目）只识别「暂停充提」。下列断言均来自 `tests/fixtures/` 中的真实标题（已实测）。

- [ ] **Step 1: 写失败的测试 `tests/test_announcements.py`**

```python
from datetime import datetime, timezone

import pytest

from tidewatch.collectors.announcements import binance_dedupe_key, classify, extract_symbols


@pytest.mark.parametrize(
    ("title", "hint", "expected"),
    [
        ("Binance Will List Hyperliquid (HYPE) with Seed Tag Applied", "listing", "spot_listing"),
        ("Binance Futures Will Launch Multiple TradFi USDⓈ-Margined Perpetual Contracts (2026-09-29)", "listing", "perp_listing"),
        ("Binance Will Add Hyperliquid (HYPE) on Earn, Buy Crypto, Convert, VIP Loan & Margin", "listing", "other"),
        ("Notice of Removal of Spot Trading Pairs - 2026-10-02", "delisting", "delisting"),
        ("OKX to list XDPUSD X-Perp", "listing", "perp_listing"),
        ("OKX to list XDP/USDT (Doppler Finance) for spot trading", "listing", "spot_listing"),
        ("OKX to Adjust KIOXIA Equity Perpetual Futures Due to Corporate Action", "listing", "other"),
        ("OKX to delist DORA, ICX, STORJ, ZEUS and ELF spot trading pairs", "delisting", "delisting"),
        ("OKX to cease deposits and withdrawals on the Lisk network", "suspension", "deposit_withdraw_suspend"),
        ("OKX to resume deposits and withdrawals on the Lisk network", "suspension", "other"),
        ("USDC on Spark (Ethereum) delisting from Onchain Earn", None, "other"),
        ("Binance Will Suspend Deposits and Withdrawals on the Solana Network", None, "deposit_withdraw_suspend"),
        ("Binance Futures Will Delist USDⓈ-M ABCUSDT Perpetual Contract", "delisting", "perp_delisting"),
    ],
)
def test_classify(title, hint, expected):
    assert classify(title, hint) == expected


@pytest.mark.parametrize(
    ("title", "symbols"),
    [
        ("Binance Will List Hyperliquid (HYPE) with Seed Tag Applied", ["HYPE"]),
        ("OKX to list XDPUSD X-Perp", ["XDP"]),
        ("OKX to list DATAUSD, SOONUSD, EIGENUSD, APEUSD X-Perps", ["DATA", "SOON", "EIGEN", "APE"]),
        ("OKX to delist DORA, ICX, STORJ, ZEUS and ELF spot trading pairs", ["DORA", "ICX", "STORJ", "ZEUS", "ELF"]),
        ("Binance Futures Will Launch OURAUSDT USDⓈ-Margined Perpetual Contract Pre-IPO Trading", ["OURA"]),
        ("Notice of Removal of Spot Trading Pairs - 2026-10-02", []),
        ("OKX to cease deposits and withdrawals on the Lisk network", []),
    ],
)
def test_extract_symbols(title, symbols):
    assert extract_symbols(title) == symbols


def test_binance_dedupe_key_same_day_same_title():
    a = binance_dedupe_key("Binance Will List XYZ (XYZ)", datetime(2026, 9, 30, 1, 0, tzinfo=timezone.utc))
    b = binance_dedupe_key("Binance Will List XYZ (XYZ) ", datetime(2026, 9, 30, 1, 0, 0, 500000, tzinfo=timezone.utc))
    c = binance_dedupe_key("Binance Will List XYZ (XYZ)", datetime(2026, 10, 1, 1, 0, tzinfo=timezone.utc))
    assert a == b and a != c and a.startswith("binance_ann:")
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_announcements.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.collectors.announcements'`

- [ ] **Step 3: 实现 `tidewatch/collectors/announcements.py`**

```python
"""交易所公告的通用处理：分类、标题币种提取、币安去重键"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import datetime, timezone

from tidewatch.pipeline.normalize import normalize_title

_PERP_WORDS = ("perpetual", "futures", "perp", "swap")
_LISTING_WORDS = ("will list", "to list", "will launch", "lists ", "listing of")
_SUSPEND_WORDS = ("suspend", "suspension", "cease", "halt", "close")

_PAREN = re.compile(r"\(([A-Z0-9]{2,15})\)")
_TOKEN = re.compile(r"(?<![A-Za-z0-9])([A-Z][A-Z0-9]{1,14})(?![A-Za-z0-9])")
_QUOTE_SUFFIXES = ("USDT", "USDC", "USD")
# 常见的非币种大写词（交易所名、法币、术语）
_STOP = {
    "OKX", "USD", "USDT", "USDC", "USDS", "API", "P2P", "NFT", "VIP", "UTC", "ETF", "FAQ", "KYC", "APR",
    "APY", "CEX", "DEX", "AMA", "OTC", "UAH", "EUR", "TRY", "BRL", "JPY", "IDR", "NGN", "ARS", "RUB",
    "GBP", "AUD", "PERP", "WEB3", "UID", "IPO",
}


def classify(title: str, hint: str | None) -> str:
    """公告分类

    Args:
        title: 英文标题
        hint: 交易所栏目给出的大类：listing / delisting / suspension / None
    Returns:
        spot_listing / perp_listing / delisting / perp_delisting / deposit_withdraw_suspend / other
    """
    lowered = title.lower()
    perp = any(word in lowered for word in _PERP_WORDS)
    if hint == "delisting":
        return "perp_delisting" if perp else "delisting"
    if hint == "listing":
        if any(word in lowered for word in _LISTING_WORDS):
            return "perp_listing" if perp else "spot_listing"
        return "other"
    if hint == "suspension":
        return "other" if "resum" in lowered else "deposit_withdraw_suspend"
    is_suspend = (
        ("deposit" in lowered or "withdraw" in lowered)
        and any(word in lowered for word in _SUSPEND_WORDS)
        and "resum" not in lowered
    )
    return "deposit_withdraw_suspend" if is_suspend else "other"


def extract_symbols(title: str) -> list[str]:
    """从英文标题中提取币种代码：优先取括号内代码，否则取大写词并去掉 USDT/USDC/USD 后缀"""
    text = unicodedata.normalize("NFKC", title)
    found = _PAREN.findall(text)
    if not found:
        for token in _TOKEN.findall(text):
            for suffix in _QUOTE_SUFFIXES:
                if token.endswith(suffix) and len(token) > len(suffix) + 1:
                    token = token[: -len(suffix)]
                    break
            found.append(token)
    return [sym for sym in dict.fromkeys(found) if sym not in _STOP and not sym.isdigit()]


def binance_dedupe_key(title: str, published_at: datetime) -> str:
    """币安公告去重键：规范化标题 + 发布日期（UTC），WS 与列表接口共用"""
    day = published_at.astimezone(timezone.utc).date().isoformat()
    digest = hashlib.sha1(f"{normalize_title(title)}|{day}".encode("utf-8")).hexdigest()
    return f"binance_ann:{digest}"
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_announcements.py -v`
Expected: `21 passed`（分类 13 个 + 币种 7 个 + 去重键 1 个）

- [ ] **Step 5: 提交**

```bash
git add tidewatch/collectors/announcements.py tests/test_announcements.py
git commit -m "feat: 交易所公告分类与币种提取" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 9: 具体采集器（金十、OKX、币安）

### Task 21: 金十快讯采集器

**Files:**
- Create: `tidewatch/collectors/jin10.py`
- Test: `tests/test_jin10.py`

接口（2026-09-30 实测，直连）：`GET https://flash-api.jin10.com/get_flash_list?channel=-8200&vip=1[&max_time=YYYY-MM-DD HH:MM:SS]`，请求头 `x-app-id: bVBF4FyRTn5NJF5n`、`x-version: 1.0.0`；返回 `{"status": 200, "data": [...]}`，每页 20 条、按时间倒序；`id` 为 20 位数字字符串（可按字符串比较大小）；`time` 为上海时间；翻页时 `max_time` 取本页最后一条时间加 1 秒（与上一页重叠，重复由去重键处理）；`type=0` 为快讯、`type=2` 为文章（带 `data.link`、`data.title`）；VIP 锁定条目 `data.lock=true`、`content` 为空，标题在 `data.vip_title`；`important` 为 0/1。夹具中第 1 条为锁定条目，第 5 条 id 为 `20260930105137078800`，最后一条时间为 `2026-09-30 10:36:49`，最大 id 为 `20260930110028586800`。

- [ ] **Step 1: 写失败的测试 `tests/test_jin10.py`**

```python
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from tidewatch.collectors.jin10 import JIN10_URL, Jin10Collector, parse_item

PAGE1 = json.loads((Path(__file__).parent / "fixtures" / "jin10" / "flash_page1.json").read_text(encoding="utf-8"))
NOW = datetime(2026, 9, 30, 3, 1, tzinfo=timezone.utc)  # 上海时间 11:01


def _collector(settings) -> Jin10Collector:
    return Jin10Collector(settings.collector("jin10"), sink=None, health=None, client=httpx.AsyncClient(trust_env=False),
                          engine=None, clock=lambda: NOW)


def test_parse_items_from_fixture():
    items = PAGE1["data"]
    locked = parse_item(items[0])
    assert locked.title == "瑞典北欧斯安银行：油市正被时间、地域和品种撕裂，现货与期货价格严重分化"
    assert locked.body == "" and locked.raw["locked"] is True
    assert locked.occurred_at == datetime(2026, 9, 30, 3, 0, 28, tzinfo=timezone.utc)
    article = parse_item(next(i for i in items if i["id"] == "20260930105126319800"))
    assert article.event_type == "article" and article.raw["important"] == 1
    assert article.url == "https://xnews.jin10.com/details/231352"
    flash = parse_item(next(i for i in items if i["id"] == "20260930105037539800"))
    assert flash.event_type == "flash" and flash.title.startswith("港股低开高走")
    assert flash.url == "https://flash.jin10.com/detail/20260930105037539800"
    assert flash.dedupe_key == "jin10:20260930105037539800"
    assert sum(parse_item(i).raw["important"] for i in items) == 6
    assert parse_item({"id": "1", "time": "2026-09-30 10:00:00", "type": 0, "data": {"content": ""}}) is None


@respx.mock
async def test_first_run_takes_first_page(settings):
    route = respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    result = await _collector(settings).fetch(None)
    assert len(result.events) == 20 and route.call_count == 1
    assert result.cursor == {"last_id": "20260930110028586800"}
    assert route.calls.last.request.headers["x-app-id"] == "bVBF4FyRTn5NJF5n"


@respx.mock
async def test_incremental_stops_at_cursor(settings):
    route = respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    result = await _collector(settings).fetch({"last_id": "20260930105137078800"})
    assert len(result.events) == 4 and route.call_count == 1


@respx.mock
async def test_paginates_until_cursor(settings):
    older = {"status": 200, "data": [
        {"id": "20260930103000000000", "time": "2026-09-30 10:30:00", "type": 0, "important": 0,
         "data": {"content": "更早的一条快讯"}},
        {"id": "20260930100000000000", "time": "2026-09-30 10:00:00", "type": 0, "important": 0,
         "data": {"content": "已经采集过的快讯"}},
    ]}
    route = respx.get(JIN10_URL).mock(side_effect=[httpx.Response(200, json=PAGE1), httpx.Response(200, json=older)])
    result = await _collector(settings).fetch({"last_id": "20260930100000000000"})
    assert len(result.events) == 21 and route.call_count == 2
    assert route.calls[1].request.url.params["max_time"] == "2026-09-30 10:36:50"
    assert result.cursor == {"last_id": "20260930110028586800"}


@respx.mock
async def test_bad_status_raises(settings):
    respx.get(JIN10_URL).mock(return_value=httpx.Response(200, json={"status": 500, "message": "错误", "data": []}))
    with pytest.raises(RuntimeError):
        await _collector(settings).fetch(None)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_jin10.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.collectors.jin10'`

- [ ] **Step 3: 实现 `tidewatch/collectors/jin10.py`**

```python
"""金十快讯采集器

金十没有公开 API，这里调用其网页使用的快讯接口（请求头与参数参照 RSSHub / newsnow 的实现），
个人低频自用。接口变化会导致解析失败，由健康监控发出告警。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from tidewatch.clock import LOCAL_TZ
from tidewatch.collectors.base import PollCollector
from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.normalize import clean_text

JIN10_URL = "https://flash-api.jin10.com/get_flash_list"
JIN10_HEADERS = {"x-app-id": "bVBF4FyRTn5NJF5n", "x-version": "1.0.0"}
MAX_PAGES = 30


def _parse_time(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL_TZ)


def parse_item(item: dict[str, Any]) -> Event | None:
    """把一条快讯转成事件；没有标题的条目返回 None"""
    data = item.get("data") or {}
    locked = bool(data.get("lock"))
    if locked:  # VIP 锁定条目只有标题
        title, body = clean_text(data.get("vip_title")), ""
    else:
        body = clean_text(data.get("content"))
        title = clean_text(data.get("title")) or body
    if not title:
        return None
    return Event(
        source="jin10",
        category="news",
        event_type="article" if item.get("type") == 2 else "flash",
        occurred_at=_parse_time(item["time"]),
        title=title[:300],
        body=body,
        url=data.get("link") or f"https://flash.jin10.com/detail/{item['id']}",
        dedupe_key=f"jin10:{item['id']}",
        raw={"id": item["id"], "important": int(item.get("important") or 0), "type": item.get("type"), "locked": locked},
    )


class Jin10Collector(PollCollector):
    name = "jin10"
    group = "news"

    async def fetch(self, cursor: dict[str, Any] | None) -> FetchResult:
        last_id = (cursor or {}).get("last_id")
        floor = self.clock() - timedelta(seconds=self.cfg.backfill_max_s)
        collected: list[dict[str, Any]] = []
        max_time: str | None = None
        for _ in range(MAX_PAGES):
            page = await self._page(max_time)
            if not page:
                break
            fresh = [item for item in page if last_id is None or item["id"] > last_id]
            collected.extend(fresh)
            # 首次运行只取第一页；本页出现已采集过的条目说明已追上游标
            if last_id is None or len(fresh) < len(page):
                break
            oldest = page[-1]
            if _parse_time(oldest["time"]) < floor:  # 超出补抓窗口
                break
            # 多取 1 秒：避免与上一页最后一条同一秒的快讯被跳过（重复由去重键处理）
            max_time = (_parse_time(oldest["time"]) + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        events = [e for e in (parse_item(item) for item in collected) if e is not None and e.occurred_at >= floor]
        newest = max((item["id"] for item in collected), default=last_id)
        return FetchResult(events, {"last_id": newest} if newest else None)

    async def _page(self, max_time: str | None) -> list[dict[str, Any]]:
        await self.limiter.acquire()
        params = {"channel": "-8200", "vip": "1"}
        if max_time:
            params["max_time"] = max_time
        response = await self.client.get(JIN10_URL, params=params, headers=JIN10_HEADERS)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != 200:
            raise RuntimeError(f"金十接口返回异常状态：{payload.get('status')} {payload.get('message')}")
        return payload.get("data") or []
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_jin10.py -v`
Expected: `5 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/collectors/jin10.py tests/test_jin10.py
git commit -m "feat: 金十快讯采集器" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 22: OKX 公告采集器

**Files:**
- Create: `tidewatch/collectors/okx_ann.py`
- Test: `tests/test_okx_ann.py`

接口（2026-09-30 实测，需代理）：`GET https://www.okx.com/api/v5/support/announcements?page=N`，返回 `{"code": "0", "data": [{"totalPage": ..., "details": [{"annType", "title", "url", "pTime", "businessPTime"}]}]}`，每页 20 条、按时间倒序，`pTime` 为毫秒字符串。夹具最新一条 `pTime=1790661613056`（Lisk 暂停充提），第二条 `pTime=1790604011303`。

- [ ] **Step 1: 写失败的测试 `tests/test_okx_ann.py`**

```python
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from tidewatch.collectors.okx_ann import OKX_URL, OkxAnnCollector, parse_detail

PAGE1 = json.loads((Path(__file__).parent / "fixtures" / "okx" / "announcements_page1.json").read_text(encoding="utf-8"))
DETAILS = PAGE1["data"][0]["details"]
NOW = datetime.fromtimestamp(1790661613056 / 1000 + 60, tz=timezone.utc)


def _collector(settings) -> OkxAnnCollector:
    return OkxAnnCollector(settings.collector("okx_ann"), sink=None, health=None, client=httpx.AsyncClient(trust_env=False),
                           engine=None, clock=lambda: NOW)


def test_parse_details():
    events = {e.title: e for e in (parse_detail(d) for d in DETAILS)}
    lisk = events["OKX to cease deposits and withdrawals on the Lisk network"]
    assert lisk.event_type == "deposit_withdraw_suspend" and lisk.raw["exchange"] == "okx"
    assert lisk.dedupe_key == "okx_ann:https://www.okx.com/help/okx-to-cease-deposits-and-withdrawals-on-the-lisk-network"
    assert lisk.occurred_at == datetime.fromtimestamp(1790661613.056, tz=timezone.utc)
    assert events["OKX to list XDPUSD X-Perp"].event_type == "perp_listing"
    assert events["OKX to list XDPUSD X-Perp"].assets == ["XDP"]
    delist = events["OKX to delist DORA, ICX, STORJ, ZEUS and ELF spot trading pairs"]
    assert delist.event_type == "delisting" and delist.assets == ["DORA", "ICX", "STORJ", "ZEUS", "ELF"]
    assert events["USDC on Spark (Ethereum) delisting from Onchain Earn"].event_type == "other"


@respx.mock
async def test_first_run_only_within_backfill_window(settings):
    route = respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    result = await _collector(settings).fetch(None)
    assert route.call_count == 1
    assert [e.title for e in result.events] == ["OKX to cease deposits and withdrawals on the Lisk network"]
    assert result.cursor == {"last_ptime": 1790661613056}


@respx.mock
async def test_incremental(settings):
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json=PAGE1))
    result = await _collector(settings).fetch({"last_ptime": 1790604011303})
    assert len(result.events) == 1 and result.cursor == {"last_ptime": 1790661613056}


@respx.mock
async def test_error_code_raises(settings):
    respx.get(OKX_URL).mock(return_value=httpx.Response(200, json={"code": "50011", "msg": "Too Many Requests"}))
    with pytest.raises(RuntimeError):
        await _collector(settings).fetch(None)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_okx_ann.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.collectors.okx_ann'`

- [ ] **Step 3: 实现 `tidewatch/collectors/okx_ann.py`**

```python
"""OKX 公告采集器（官方公开接口）"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from tidewatch.collectors.announcements import classify, extract_symbols
from tidewatch.collectors.base import PollCollector
from tidewatch.models import Event, FetchResult

OKX_URL = "https://www.okx.com/api/v5/support/announcements"
MAX_PAGES = 5
# annType → 分类大类
_HINTS = {
    "announcements-new-listings": "listing",
    "announcements-delistings": "delisting",
    "announcements-deposit-withdrawal-suspension-resumption": "suspension",
}


def parse_detail(detail: dict[str, Any]) -> Event:
    title = detail["title"].strip()
    ann_type = detail.get("annType", "")
    return Event(
        source="okx_ann",
        category="exchange",
        event_type=classify(title, _HINTS.get(ann_type)),
        occurred_at=datetime.fromtimestamp(int(detail["pTime"]) / 1000, tz=timezone.utc),
        title=title,
        url=detail["url"],
        dedupe_key=f"okx_ann:{detail['url']}",
        assets=extract_symbols(title),
        raw={"exchange": "okx", "annType": ann_type, "pTime": detail["pTime"]},
    )


class OkxAnnCollector(PollCollector):
    name = "okx_ann"
    group = "news"

    async def fetch(self, cursor: dict[str, Any] | None) -> FetchResult:
        last = int((cursor or {}).get("last_ptime", 0))
        floor_ms = int((self.clock() - timedelta(seconds=self.cfg.backfill_max_s)).timestamp() * 1000)
        collected: list[dict[str, Any]] = []
        for page in range(1, MAX_PAGES + 1):
            items = await self._page(page)
            collected.extend(items)
            # 本页全部比游标新、且仍在补抓窗口内时才继续翻页；首次运行只取第一页
            if (
                not items
                or cursor is None
                or any(int(item["pTime"]) <= last for item in items)
                or int(items[-1]["pTime"]) < floor_ms
            ):
                break
        # 只按补抓窗口过滤、不按游标过滤：延迟出现的旧公告也能采到，重复由去重键处理
        events = [parse_detail(item) for item in collected if int(item["pTime"]) >= floor_ms]
        newest = max((int(item["pTime"]) for item in collected), default=last)
        return FetchResult(events, {"last_ptime": newest} if newest else None)

    async def _page(self, page: int) -> list[dict[str, Any]]:
        await self.limiter.acquire()
        response = await self.client.get(OKX_URL, params={"page": page})
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX 公告接口返回错误：{payload.get('code')} {payload.get('msg')}")
        data = payload.get("data") or []
        return (data[0].get("details") if data else None) or []
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_okx_ann.py -v`
Expected: `4 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/collectors/okx_ann.py tests/test_okx_ann.py
git commit -m "feat: OKX 公告采集器" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 23: 币安公告采集器（WS 与降级轮询）

**Files:**
- Create: `tidewatch/collectors/binance_ann.py`
- Test: `tests/test_binance_ann.py`

接口（2026-09-30 实测列表接口，需代理）：
- 列表：`GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId={48|161}&pageNo=1&pageSize=20`，返回 `{"success": true, "data": {"catalogs": [{"catalogId", "catalogName", "articles": [{"id", "code", "title", "type", "releaseDate"}]}]}}`；48 为新币上线、161 为下架。夹具（各 10 条）中：161 最新 `releaseDate=1790673300000`，其次 `1790668801341`；48 最新 `1790657101338`。
- WS（官方文档）：`wss://api.binance.com/sapi/wss?random=&recvWindow=&timestamp=&topic=com_announcement_en&signature=`，签名为对按键名排序的查询串做 HMAC-SHA256（密钥为 API Secret，十六进制）；请求头 `X-MBX-APIKEY`；连接后发送 `{"command":"SUBSCRIBE","value":"com_announcement_en"}`；数据消息为 `{"type":"DATA","topic":...,"data":"<JSON 字符串>"}`，其中含 `catalogId`、`catalogName`、`publishDate`（毫秒）、`title`、`body`；每 30 秒需发 PING（由 websockets 的 `ping_interval=30` 自动完成），连接最长 24 小时。
- 有 API Key 与 Secret 时用 `BinanceAnnStream`（WS + 列表补缺），否则用 `BinanceAnnPoll`（每 `interval_s` 轮询列表，状态页显示降级模式，由 Task 27 装配时设置）。

- [ ] **Step 1: 写失败的测试 `tests/test_binance_ann.py`**

```python
import asyncio
import hashlib
import hmac
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import respx
from websockets.asyncio.server import serve

from tidewatch.collectors.announcements import binance_dedupe_key
from tidewatch.collectors.binance_ann import (
    BINANCE_LIST_URL,
    BinanceAnnPoll,
    BinanceAnnStream,
    parse_article,
    parse_ws_message,
    signed_ws_url,
)

FIX = Path(__file__).parent / "fixtures" / "binance"
LIST48 = json.loads((FIX / "list_48.json").read_text(encoding="utf-8"))
LIST161 = json.loads((FIX / "list_161.json").read_text(encoding="utf-8"))
NOW = datetime.fromtimestamp(1790673300000 / 1000 + 60, tz=timezone.utc)
ANN = {"catalogId": 48, "catalogName": "New Cryptocurrency Listing", "publishDate": 1790673000000,
       "title": "Binance Will List Example (EXM)", "body": "<p>Binance will list EXM.</p>", "disclaimer": ""}


def _articles(payload):
    return payload["data"]["catalogs"][0]["articles"]


def test_parse_list_articles():
    hype = parse_article(next(a for a in _articles(LIST48) if a["title"].startswith("Binance Will List Hyperliquid")), 48)
    assert hype.event_type == "spot_listing" and hype.assets == ["HYPE"] and hype.raw["exchange"] == "binance"
    assert hype.url.startswith("https://www.binance.com/en/support/announcement/")
    perp = parse_article(_articles(LIST48)[0], 48)
    assert perp.event_type == "perp_listing"
    removal = parse_article(_articles(LIST161)[0], 161)
    assert removal.event_type == "delisting" and removal.assets == []
    assert removal.dedupe_key == binance_dedupe_key(removal.title, removal.occurred_at)


def test_ws_message_parsing_and_key_matches_list():
    message = json.dumps({"type": "DATA", "topic": "com_announcement_en", "data": json.dumps(ANN)})
    event = parse_ws_message(message)
    assert event.title == "Binance Will List Example (EXM)" and event.event_type == "spot_listing"
    assert event.assets == ["EXM"] and event.body == "Binance will list EXM."
    article = {"id": 1, "code": "abc", "title": "Binance Will List Example (EXM)", "releaseDate": 1790673000500}
    assert parse_article(article, 48).dedupe_key == event.dedupe_key  # WS 与列表接口键一致
    assert parse_ws_message(json.dumps({"type": "COMMAND", "data": "SUCCESS", "subType": "SUBSCRIBE"})) is None


def test_signed_ws_url():
    url = signed_ws_url("secret", 1790700000000, "abc", base_url="wss://example/sapi/wss")
    query = "random=abc&recvWindow=30000&timestamp=1790700000000&topic=com_announcement_en"
    expected = hmac.new(b"secret", query.encode(), hashlib.sha256).hexdigest()
    assert url == f"wss://example/sapi/wss?{query}&signature={expected}"


@respx.mock
async def test_poll_fetch_list_within_window(settings):
    respx.get(BINANCE_LIST_URL).mock(
        side_effect=lambda request: httpx.Response(200, json=LIST48 if request.url.params["catalogId"] == "48" else LIST161)
    )
    cfg = settings.collector("binance_ann").model_copy(update={"rate_per_s": 100})  # 测试中不等待限流
    collector = BinanceAnnPoll(cfg, sink=None, health=None,
                               client=httpx.AsyncClient(trust_env=False), engine=None, clock=lambda: NOW)
    result = await collector.fetch(None)
    assert len(result.events) == 3  # 161 两条 + 48 一条落在 6 小时补抓窗口内
    assert result.cursor == {"last_ms": 1790673300000}
    again = await collector.fetch(result.cursor)
    # 只按补抓窗口过滤：再次拉取仍返回窗口内条目，由入库去重保证只入库一次
    assert [e.dedupe_key for e in again.events] == [e.dedupe_key for e in result.events]
    assert again.cursor == {"last_ms": 1790673300000}


async def test_stream_with_local_server(settings):
    received = {}

    async def handler(ws):
        received["header"] = ws.request.headers.get("X-MBX-APIKEY")
        received["command"] = json.loads(await ws.recv())
        await ws.send(json.dumps({"type": "COMMAND", "data": "SUCCESS", "subType": "SUBSCRIBE", "code": "00000000"}))
        await ws.send(json.dumps({"type": "DATA", "topic": "com_announcement_en", "data": json.dumps(ANN)}))
        await ws.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        collector = BinanceAnnStream(settings.collector("binance_ann"), api_key="k", api_secret="s",
                                     ws_url=f"ws://127.0.0.1:{port}/sapi/wss", sink=None, health=None,
                                     client=None, engine=None, clock=lambda: NOW)
        stream = collector.stream(None)
        first = await asyncio.wait_for(stream.__anext__(), timeout=5)
        second = await asyncio.wait_for(stream.__anext__(), timeout=5)
        await stream.aclose()
    assert received["header"] == "k"
    assert received["command"] == {"command": "SUBSCRIBE", "value": "com_announcement_en"}
    assert first.events == [] and first.cursor is None  # 订阅确认按心跳处理
    assert second.events[0].title == ANN["title"] and second.cursor == {"last_ms": 1790673000000}
    assert collector.merge_cursor({"last_ms": 5}, {"last_ms": 3}) == {"last_ms": 5}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_binance_ann.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.collectors.binance_ann'`

- [ ] **Step 3: 实现 `tidewatch/collectors/binance_ann.py`**

```python
"""币安公告采集器

- BinanceAnnStream：有只读 API Key 时订阅公告 WS，并用列表接口在（重）连接后及每 10 分钟补缺
- BinanceAnnPoll：没有 API Key 时的降级模式，定时轮询列表接口
两种来源使用同一个去重键（规范化标题 + 发布日期），同一公告只入库一次。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator
from urllib.parse import urlencode

from websockets.asyncio.client import connect

from tidewatch.collectors.announcements import binance_dedupe_key, classify, extract_symbols
from tidewatch.collectors.base import PollCollector, StreamCollector
from tidewatch.config import CollectorCfg
from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.normalize import clean_text

BINANCE_LIST_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
BINANCE_WS_URL = "wss://api.binance.com/sapi/wss"
WS_TOPIC = "com_announcement_en"
# 列表接口轮询的栏目及其分类大类
LIST_CATALOGS = {48: "listing", 161: "delisting"}
WS_RECV_TIMEOUT_S = 60


def _published(ms: int | str) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def _make_event(
    title: str, published: datetime, catalog_id: int | None, *, url: str | None, body: str = "", extra: dict | None = None
) -> Event:
    title = title.strip()
    return Event(
        source="binance_ann",
        category="exchange",
        event_type=classify(title, LIST_CATALOGS.get(catalog_id)),
        occurred_at=published,
        title=title,
        body=body,
        url=url,
        dedupe_key=binance_dedupe_key(title, published),
        assets=extract_symbols(title),
        raw={"exchange": "binance", "catalogId": catalog_id, **(extra or {})},
    )


def parse_article(article: dict[str, Any], catalog_id: int) -> Event:
    """列表接口的一篇公告 → 事件"""
    return _make_event(
        article["title"],
        _published(article["releaseDate"]),
        catalog_id,
        url=f"https://www.binance.com/en/support/announcement/{article['code']}",
        extra={"code": article.get("code")},
    )


def parse_ws_message(raw: str | bytes) -> Event | None:
    """WS 消息 → 事件；订阅确认等非数据消息返回 None"""
    message = json.loads(raw)
    if message.get("type") != "DATA":
        return None
    data = message.get("data")
    if isinstance(data, str):
        data = json.loads(data)
    if not data or not data.get("title"):
        return None
    catalog_id = data.get("catalogId")
    return _make_event(
        data["title"],
        _published(data["publishDate"]),
        catalog_id,
        url=f"https://www.binance.com/en/support/announcement/list/{catalog_id}" if catalog_id else None,
        body=clean_text(data.get("body")),
        extra={"catalogName": data.get("catalogName")},
    )


def signed_ws_url(api_secret: str, now_ms: int, nonce: str, *, base_url: str = BINANCE_WS_URL) -> str:
    """生成带签名的 WS 地址：对按键名排序的查询串做 HMAC-SHA256"""
    params = {"random": nonce, "recvWindow": 30000, "timestamp": now_ms, "topic": WS_TOPIC}
    query = urlencode(sorted(params.items()))
    signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{base_url}?{query}&signature={signature}"


class _BinanceList:
    """列表接口：每个栏目取最新 20 条（混入类，依赖采集器基类的 client、limiter、clock、cfg）"""

    async def fetch_list(self, cursor: dict[str, Any] | None) -> FetchResult:
        last = int((cursor or {}).get("last_ms", 0))
        floor_ms = int((self.clock() - timedelta(seconds=self.cfg.backfill_max_s)).timestamp() * 1000)
        events: list[Event] = []
        newest = last
        for catalog_id in LIST_CATALOGS:
            await self.limiter.acquire()
            response = await self.client.get(
                BINANCE_LIST_URL, params={"type": 1, "catalogId": catalog_id, "pageNo": 1, "pageSize": 20}
            )
            response.raise_for_status()
            payload = response.json()
            if not payload.get("success"):
                raise RuntimeError(f"币安公告接口返回错误：{payload.get('code')} {payload.get('message')}")
            catalogs = (payload.get("data") or {}).get("catalogs") or []
            for article in (catalogs[0].get("articles") if catalogs else None) or []:
                published_ms = int(article["releaseDate"])
                newest = max(newest, published_ms)
                # 只按补抓窗口过滤、不按游标过滤：两个栏目共用一个游标，若按游标过滤，
                # 某栏目延迟出现的较早公告会被永久跳过；重复条目由去重键处理
                if published_ms >= floor_ms:
                    events.append(parse_article(article, catalog_id))
        return FetchResult(events, {"last_ms": newest} if newest else None)


class BinanceAnnPoll(_BinanceList, PollCollector):
    """降级模式：没有 API Key 时轮询列表接口"""

    name = "binance_ann"
    group = "news"

    async def fetch(self, cursor: dict[str, Any] | None) -> FetchResult:
        return await self.fetch_list(cursor)


class BinanceAnnStream(_BinanceList, StreamCollector):
    """WS 模式：订阅公告推送，列表接口补缺"""

    name = "binance_ann"
    group = "news"

    def __init__(
        self,
        cfg: CollectorCfg,
        *,
        api_key: str,
        api_secret: str,
        ws_proxy: str | None = None,
        ws_url: str = BINANCE_WS_URL,
        **kwargs: Any,
    ) -> None:
        super().__init__(cfg, **kwargs)
        self._api_key = api_key
        self._api_secret = api_secret
        self._ws_proxy = ws_proxy
        self._ws_url = ws_url

    async def gap_fill(self, cursor: dict[str, Any] | None) -> FetchResult:
        return await self.fetch_list(cursor)

    def merge_cursor(self, old: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any] | None:
        if new is None:
            return old
        return {"last_ms": max(int((old or {}).get("last_ms", 0)), int(new["last_ms"]))}

    async def stream(self, cursor: dict[str, Any] | None) -> AsyncIterator[FetchResult]:
        url = signed_ws_url(self._api_secret, int(self.clock().timestamp() * 1000), uuid.uuid4().hex, base_url=self._ws_url)
        async with connect(
            url,
            additional_headers={"X-MBX-APIKEY": self._api_key},
            proxy=self._ws_proxy,
            ping_interval=30,
            ping_timeout=20,
            open_timeout=20,
        ) as ws:
            await ws.send(json.dumps({"command": "SUBSCRIBE", "value": WS_TOPIC}))
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT_S)
                except asyncio.TimeoutError:
                    # 一段时间没有公告：连接仍由 websockets 自动 ping 保活，按心跳处理
                    yield FetchResult([], None)
                    continue
                event = parse_ws_message(raw)
                if event is None:
                    yield FetchResult([], None)
                else:
                    yield FetchResult([event], {"last_ms": int(event.occurred_at.timestamp() * 1000)})
```

- [ ] **Step 4: 运行测试与全量回归**

Run: `pytest tests/test_binance_ann.py -v && pytest -q`
Expected: `5 passed`；全量回归全部通过（0 failed）

- [ ] **Step 5: 提交**

```bash
git add tidewatch/collectors/binance_ann.py tests/test_binance_ann.py
git commit -m "feat: 币安公告采集器（WS 订阅与降级轮询）" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 10: 网站（广播、查询、页面）

### Task 24: 网站查询

**Files:**
- Create: `tidewatch/web/__init__.py`（空文件）
- Create: `tidewatch/web/queries.py`
- Test: `tests/test_web_queries.py`

时间线以簇为单位，代表事件：已推送的簇取 `rep_event_id`，否则取簇内 `final_level` 最高、发生时间最早的一条（系统设计 6.2、规范 web-dashboard）。等级展示与筛选用簇的 `max_level`（即 `final_level` 的最高值）；默认隐藏 C 级；资产筛选看簇内任一事件；未推送且「采集时间 − 发生时间」超过该源新鲜度的簇标记「补抓」。

- [ ] **Step 1: 写失败的测试 `tests/test_web_queries.py`**

```python
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import insert, update

from tidewatch.pipeline.decision import freshness_from_settings
from tidewatch.store.tables import collector_state, event_clusters, events, push_log
from tidewatch.web.queries import cluster_rows, collector_status, push_stats, timeline

NOW = datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)


async def _cluster(conn, level, *, committed=None, category="news") -> int:
    return (await conn.execute(insert(event_clusters).values(
        category=category, first_seen_at=NOW, last_seen_at=NOW, max_level=level, committed_level=committed,
    ).returning(event_clusters.c.id))).scalar_one()


async def _event(conn, cid, level, title, *, minutes_ago=1, collected_delay=0, assets=None, source="jin10") -> int:
    occurred = NOW - timedelta(minutes=minutes_ago)
    return (await conn.execute(insert(events).values(
        source=source, category="news", event_type="flash", occurred_at=occurred,
        collected_at=occurred + timedelta(minutes=collected_delay), title=title,
        dedupe_key=f"t:{uuid.uuid4().hex}", final_level=level, assets=assets or [], cluster_id=cid,
    ).returning(events.c.id))).scalar_one()


async def test_timeline_representative_filters_and_backfill(engine, settings):
    fresh = freshness_from_settings(settings)
    async with engine.begin() as conn:
        c1 = await _cluster(conn, "S")
        await _event(conn, c1, "B", "B 级先到", minutes_ago=5)
        await _event(conn, c1, "S", "S 级后到", minutes_ago=3, assets=["BTC"])
        c2 = await _cluster(conn, "C")
        await _event(conn, c2, "C", "C 级快讯", minutes_ago=2)
        c3 = await _cluster(conn, "A")
        await _event(conn, c3, "A", "补抓的旧快讯", minutes_ago=200, collected_delay=190)
        c4 = await _cluster(conn, "A", committed="A")
        b_id = await _event(conn, c4, "B", "已推送簇的代表", minutes_ago=4)
        await _event(conn, c4, "A", "同簇另一条", minutes_ago=6)
        await conn.execute(update(event_clusters).where(event_clusters.c.id == c4).values(rep_event_id=b_id))

    rows = await timeline(engine, freshness=fresh)
    titles = [r["title"] for r in rows]
    assert titles == ["S 级后到", "已推送簇的代表", "补抓的旧快讯"]  # 按代表事件发生时间倒序，默认隐藏 C
    by_title = {r["title"]: r for r in rows}
    assert by_title["补抓的旧快讯"]["backfill"] is True and by_title["S 级后到"]["backfill"] is False
    assert by_title["已推送簇的代表"]["pushed"] is True and by_title["S 级后到"]["level"] == "S"
    assert by_title["S 级后到"]["time_local"] == "09-30 09:57:00" and by_title["S 级后到"]["source_name"] == "金十快讯"

    assert [r["title"] for r in await timeline(engine, freshness=fresh, include_c=True)][0] == "C 级快讯"  # 发生时间最新
    assert [r["title"] for r in await timeline(engine, freshness=fresh, level="C")] == ["C 级快讯"]
    assert [r["title"] for r in await timeline(engine, freshness=fresh, asset="BTC")] == ["S 级后到"]
    assert await timeline(engine, freshness=fresh, category="exchange") == []
    assert [r["cluster_id"] for r in await cluster_rows(engine, [c1, c2], freshness=fresh)] == [c2, c1]  # 按发生时间倒序


async def test_collector_status_and_push_stats(engine):
    async with engine.begin() as conn:
        await conn.execute(insert(collector_state).values(
            name="jin10", status="abnormal", mode="normal", consecutive_failures=5, last_error="HTTP 503",
            last_success_at=NOW - timedelta(minutes=10)))
        await conn.execute(insert(collector_state).values(name="binance_ann", status="normal", mode="degraded"))
        cid = await _cluster(conn, "A")
        eid = await _event(conn, cid, "A", "推送统计")
        for status in ("ok", "ok", "failed", "dry_run"):
            await conn.execute(insert(push_log).values(
                cluster_id=cid, event_id=eid, level="A", kind="event", status=status,
                retry_until=NOW + timedelta(hours=1), created_at=NOW))
        await conn.execute(insert(push_log).values(  # 前一天（上海时间）的记录不计入
            cluster_id=cid, event_id=eid, level="A", kind="event", status="ok",
            retry_until=NOW, created_at=NOW - timedelta(days=1)))
    rows = {r["name"]: r for r in await collector_status(engine)}
    assert rows["jin10"]["status_text"] == "异常" and rows["jin10"]["last_error"] == "HTTP 503"
    assert rows["jin10"]["source_name"] == "金十快讯" and rows["jin10"]["last_success"] == "09:50:00"
    assert rows["binance_ann"]["mode_text"] == "降级"
    stats = await push_stats(engine, NOW)
    assert stats["ok"] == 2 and stats["failed"] == 1 and stats["dry_run"] == 1 and stats["expired"] == 0
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mkdir -p tidewatch/web && touch tidewatch/web/__init__.py && pytest tests/test_web_queries.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.web.queries'`

- [ ] **Step 3: 实现 `tidewatch/web/queries.py`**

```python
"""网站查询：时间线（以簇为单位）与状态页"""

from __future__ import annotations

from datetime import datetime, time as dtime
from typing import Any

from sqlalchemy import case, exists, func, select, true
from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.clock import LOCAL_TZ, format_local
from tidewatch.notify.base import source_name
from tidewatch.pipeline.decision import FreshnessLookup
from tidewatch.store.tables import collector_state, event_clusters, events, push_log

LEVELS = ("S", "A", "B", "C")
STATUS_TEXT = {"normal": "正常", "abnormal": "异常", "unknown": "未知"}
PUSH_STATUSES = ("ok", "dry_run", "failed", "expired", "pending", "superseded")

_RANK = case({"S": 3, "A": 2, "B": 1}, value=events.c.final_level, else_=0)


def _cluster_select():
    """簇 + 代表事件（LATERAL 子查询：已推送取 rep_event_id，否则取等级最高、最早的一条）"""
    rep = (
        select(
            events.c.id, events.c.title, events.c.url, events.c.source,
            events.c.occurred_at, events.c.collected_at, events.c.assets,
        )
        .where(events.c.cluster_id == event_clusters.c.id)
        .order_by(
            (events.c.id == event_clusters.c.rep_event_id).desc().nulls_last(),
            _RANK.desc(),
            events.c.occurred_at,
            events.c.id,
        )
        .limit(1)
        .lateral("rep")
    )
    stmt = select(
        event_clusters.c.id.label("cluster_id"),
        event_clusters.c.category,
        event_clusters.c.max_level,
        event_clusters.c.committed_level,
        event_clusters.c.source_count,
        rep.c.id.label("event_id"),
        rep.c.title,
        rep.c.url,
        rep.c.source,
        rep.c.occurred_at,
        rep.c.collected_at,
        rep.c.assets,
    ).select_from(event_clusters.join(rep, true()))
    return stmt, rep


def _to_dict(row: Any, freshness: FreshnessLookup) -> dict[str, Any]:
    max_age = freshness(row.source)
    stale = max_age is not None and (row.collected_at - row.occurred_at) > max_age
    return {
        "cluster_id": row.cluster_id,
        "event_id": row.event_id,
        "category": row.category,
        "level": row.max_level,
        "title": row.title,
        "url": row.url,
        "source": row.source,
        "source_name": source_name(row.source),
        "source_count": row.source_count,
        "assets": list(row.assets),
        "occurred_at": row.occurred_at.isoformat(),
        "time_local": format_local(row.occurred_at, "%m-%d %H:%M:%S"),
        "pushed": row.committed_level is not None,
        "backfill": row.committed_level is None and stale,
    }


async def timeline(
    engine: AsyncEngine,
    *,
    freshness: FreshnessLookup,
    category: str | None = None,
    level: str | None = None,
    asset: str | None = None,
    include_c: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """首页时间线：按代表事件的发生时间倒序"""
    stmt, rep = _cluster_select()
    if category:
        stmt = stmt.where(event_clusters.c.category == category)
    if level in LEVELS:
        stmt = stmt.where(event_clusters.c.max_level == level)
    elif not include_c:
        stmt = stmt.where(event_clusters.c.max_level != "C")
    if asset:
        stmt = stmt.where(
            exists().where(events.c.cluster_id == event_clusters.c.id, events.c.assets.contains([asset]))
        )
    stmt = stmt.order_by(rep.c.occurred_at.desc()).limit(limit)
    async with engine.connect() as conn:
        rows = (await conn.execute(stmt)).all()
    return [_to_dict(row, freshness) for row in rows]


async def cluster_rows(engine: AsyncEngine, cluster_ids: list[int], *, freshness: FreshnessLookup) -> list[dict[str, Any]]:
    """指定簇的时间线行（用于 SSE 增量更新），按代表事件发生时间倒序"""
    if not cluster_ids:
        return []
    stmt, rep = _cluster_select()
    stmt = stmt.where(event_clusters.c.id.in_(cluster_ids)).order_by(rep.c.occurred_at.desc())
    async with engine.connect() as conn:
        rows = (await conn.execute(stmt)).all()
    return [_to_dict(row, freshness) for row in rows]


def _local_time(value: datetime | None) -> str:
    if value is None:
        return "—"
    return format_local(value, "%H:%M:%S")


async def collector_status(engine: AsyncEngine) -> list[dict[str, Any]]:
    """状态页：各采集器的健康状态"""
    async with engine.connect() as conn:
        rows = (await conn.execute(select(collector_state).order_by(collector_state.c.name))).mappings().all()
    return [
        {
            "name": row["name"],
            "source_name": source_name(row["name"]),
            "status": row["status"],
            "status_text": STATUS_TEXT.get(row["status"], row["status"]),
            "mode": row["mode"],
            "mode_text": "降级" if row["mode"] == "degraded" else "正常",
            "last_success": _local_time(row["last_success_at"]),
            "last_event": _local_time(row["last_event_at"]),
            "consecutive_failures": row["consecutive_failures"],
            "last_error": row["last_error"] or "",
        }
        for row in rows
    ]


async def push_stats(engine: AsyncEngine, now: datetime) -> dict[str, int]:
    """状态页：当天（上海时间）按状态统计的推送数"""
    day_start = datetime.combine(now.astimezone(LOCAL_TZ).date(), dtime.min, tzinfo=LOCAL_TZ)
    stmt = (
        select(push_log.c.status, func.count())
        .where(push_log.c.created_at >= day_start)
        .group_by(push_log.c.status)
    )
    async with engine.connect() as conn:
        counts = dict((await conn.execute(stmt)).all())
    return {status: int(counts.get(status, 0)) for status in PUSH_STATUSES}
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_web_queries.py -v`
Expected: `2 passed`

- [ ] **Step 5: 提交**

```bash
git add tidewatch/web tests/test_web_queries.py
git commit -m "feat: 网站时间线与状态查询" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 25: SSE 广播与实时更新

**Files:**
- Create: `tidewatch/web/broadcaster.py`
- Create: `tidewatch/web/live.py`
- Test: `tests/test_web_live.py`

`Broadcaster` 是进程内的发布订阅：每个 SSE 连接一个队列，队列满时丢弃最旧的消息。`LiveUpdates` 把 `EventSink` 与 `HealthMonitor` 的同步回调转成 SSE 消息：`event.created`（新事件 id）、`cluster.updated`（簇的时间线行，由后台协程查询后发布）、`health.changed`（采集器状态）。

- [ ] **Step 1: 写失败的测试 `tests/test_web_live.py`**

```python
import asyncio
import json
from datetime import timedelta

from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.live import LiveUpdates


async def _next(agen):
    return await asyncio.wait_for(agen.__anext__(), timeout=2)


async def test_publish_subscribe_and_overflow():
    broadcaster = Broadcaster(max_queue=2)
    agen = broadcaster.subscribe()
    first = asyncio.create_task(_next(agen))
    while broadcaster.subscriber_count == 0:  # 等订阅者完成注册（3.11 的 wait_for 会再包一层任务）
        await asyncio.sleep(0)
    broadcaster.publish("cluster.updated", {"cluster_id": 1, "title": "中文标题"})
    message = await first
    assert message["event"] == "cluster.updated"
    assert json.loads(message["data"]) == {"cluster_id": 1, "title": "中文标题"}
    for i in range(3):  # 队列容量 2：第 1 条被丢弃
        broadcaster.publish("tick", {"i": i})
    assert json.loads((await _next(agen))["data"]) == {"i": 1}
    await agen.aclose()
    assert broadcaster.subscriber_count == 0


async def test_live_updates_from_sink_and_health(engine, settings, clock):
    broadcaster = Broadcaster()
    live = LiveUpdates(engine, broadcaster, settings)
    sink = create_event_sink(engine, settings, clock=clock)
    sink.on_ingest(live.on_ingest)
    agen = broadcaster.subscribe()
    pending = asyncio.create_task(_next(agen))
    while broadcaster.subscriber_count == 0:
        await asyncio.sleep(0)
    await sink.start()
    try:
        event = Event(source="jin10", category="news", event_type="flash", occurred_at=clock.now - timedelta(minutes=1),
                      title="美国9月非农就业人口增加8.2万人", dedupe_key="jin10:1", raw={"important": 1})
        await sink.ingest(FetchResult([event], None), collector="jin10")
    finally:
        await sink.stop()
    created = await pending
    assert created["event"] == "event.created"
    await live.process_once()  # 处理一批待刷新的簇
    updated = await _next(agen)
    row = json.loads(updated["data"])
    assert updated["event"] == "cluster.updated" and row["level"] == "S" and row["pushed"] is True
    live.on_health({"name": "jin10", "status": "abnormal"})
    health = await _next(agen)
    assert health["event"] == "health.changed" and json.loads(health["data"])["status"] == "abnormal"
    await agen.aclose()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_web_live.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.web.broadcaster'`

- [ ] **Step 3: 实现 `tidewatch/web/broadcaster.py`**

```python
"""进程内 SSE 广播"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator


class Broadcaster:
    """每个订阅者（一个 SSE 连接）一个队列；队列满时丢弃最旧的消息，避免慢连接拖垮进程"""

    def __init__(self, max_queue: int = 500) -> None:
        self._max_queue = max_queue
        self._subscribers: set[asyncio.Queue[tuple[str, dict[str, Any]]]] = set()

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def publish(self, event: str, data: dict[str, Any]) -> None:
        for queue in list(self._subscribers):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait((event, data))

    async def subscribe(self) -> AsyncIterator[dict[str, str]]:
        """产出 sse-starlette 可直接发送的 {"event", "data"} 字典"""
        queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=self._max_queue)
        self._subscribers.add(queue)
        try:
            while True:
                event, data = await queue.get()
                yield {"event": event, "data": json.dumps(data, ensure_ascii=False, default=str)}
        finally:
            self._subscribers.discard(queue)
```

- [ ] **Step 4: 实现 `tidewatch/web/live.py`**

```python
"""实时更新：把流水线与健康监控的回调转成 SSE 消息"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from tidewatch.pipeline.decision import freshness_from_settings
from tidewatch.pipeline.sink import IngestOutcome
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.queries import cluster_rows

logger = logging.getLogger(__name__)


class LiveUpdates:
    def __init__(self, engine: AsyncEngine, broadcaster: Broadcaster, settings: Any) -> None:
        self._engine = engine
        self._broadcaster = broadcaster
        self._freshness = freshness_from_settings(settings)
        self._pending: asyncio.Queue[list[int]] = asyncio.Queue()

    def on_ingest(self, outcome: IngestOutcome) -> None:
        """EventSink 提交后的同步回调：先发 event.created，簇行查询交给后台协程"""
        self._broadcaster.publish("event.created", {"event_ids": outcome.event_ids})
        self._pending.put_nowait(sorted(set(outcome.cluster_ids)))

    def on_health(self, payload: dict[str, Any]) -> None:
        self._broadcaster.publish("health.changed", payload)

    async def process_once(self) -> None:
        """处理队列中所有待刷新的簇"""
        ids: set[int] = set()
        while not self._pending.empty():
            ids.update(self._pending.get_nowait())
        for row in await cluster_rows(self._engine, sorted(ids), freshness=self._freshness):
            self._broadcaster.publish("cluster.updated", row)

    async def run(self) -> None:
        while True:
            self._pending.put_nowait(await self._pending.get())  # 等待至少一批
            try:
                await self.process_once()
            except Exception:
                logger.exception("实时更新失败")
```

- [ ] **Step 5: 运行测试**

Run: `pytest tests/test_web_live.py -v`
Expected: `2 passed`

- [ ] **Step 6: 提交**

```bash
git add tidewatch/web/broadcaster.py tidewatch/web/live.py tests/test_web_live.py
git commit -m "feat: SSE 广播与实时更新" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 26: 网站应用、页面与前端脚本

**Files:**
- Create: `tidewatch/web/server.py`
- Create: `tidewatch/web/templates/base.html`、`index.html`、`_row.html`、`status.html`
- Create: `tidewatch/web/static/app.js`、`tidewatch/web/static/style.css`
- Test: `tests/test_web_server.py`

页面：`/` 时间线（筛选：类别、等级、资产、显示 C 级）；`/status` 状态页；`/api/timeline`、`/api/status` 为 JSON；`/fragment/cluster/{id}` 返回单行 HTML（前端收到 `cluster.updated` 后拉取该片段插入或替换，渲染逻辑只在服务端一处）；`/stream` 为 SSE。

- [ ] **Step 1: 写失败的测试 `tests/test_web_server.py`**

```python
from datetime import timedelta

import httpx
import pytest

from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.server import create_app


@pytest.fixture
async def seeded(engine, settings, clock):
    sink = create_event_sink(engine, settings, clock=clock)
    await sink.start()
    events = [
        ("jin10:1", "美国9月非农就业人口增加8.2万人", 1),
        ("jin10:2", "比特币突破12万美元", 0),
        ("jin10:3", "印度银行提供新服务", 0),
    ]
    await sink.ingest(FetchResult([
        Event(source="jin10", category="news", event_type="flash", occurred_at=clock.now - timedelta(minutes=i + 1),
              title=title, dedupe_key=key, raw={"important": important})
        for i, (key, title, important) in enumerate(events)
    ], {"last_id": "3"}), collector="jin10")
    await sink.stop()
    return engine


@pytest.fixture
async def client(seeded, settings, clock):
    app = create_app(seeded, Broadcaster(), settings, clock=clock)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        yield http


async def test_index_hides_c_by_default(client):
    page = await client.get("/")
    assert page.status_code == 200
    assert "美国9月非农" in page.text and "比特币突破" in page.text and "印度银行" not in page.text
    shown = await client.get("/", params={"show_c": "1"})
    assert "印度银行" in shown.text


async def test_api_timeline_filters(client):
    rows = (await client.get("/api/timeline", params={"asset": "BTC"})).json()
    assert [r["title"] for r in rows] == ["比特币突破12万美元"]
    rows = (await client.get("/api/timeline", params={"level": "S"})).json()
    assert [r["level"] for r in rows] == ["S"] and rows[0]["pushed"] is True


async def test_fragment_and_status(client):
    rows = (await client.get("/api/timeline")).json()
    fragment = await client.get(f"/fragment/cluster/{rows[0]['cluster_id']}")
    assert fragment.status_code == 200 and fragment.text.strip().startswith("<tr")
    assert (await client.get("/fragment/cluster/999999")).status_code == 404
    status = await client.get("/status")
    assert status.status_code == 200 and "金十快讯" in status.text
    data = (await client.get("/api/status")).json()
    assert data["collectors"][0]["name"] == "jin10" and data["push_stats"]["pending"] == 1
    assert (await client.get("/static/app.js")).status_code == 200
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_web_server.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.web.server'`

- [ ] **Step 3: 实现 `tidewatch/web/server.py`**

```python
"""网站：FastAPI 应用（时间线、状态页、SSE、行片段）"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncEngine
from sse_starlette.sse import EventSourceResponse

from tidewatch.clock import Clock, utcnow
from tidewatch.pipeline.decision import freshness_from_settings
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.queries import LEVELS, cluster_rows, collector_status, push_stats, timeline

WEB_DIR = Path(__file__).resolve().parent


def create_app(engine: AsyncEngine, broadcaster: Broadcaster, settings: Any, *, clock: Clock = utcnow) -> FastAPI:
    app = FastAPI(title="观潮", docs_url=None, redoc_url=None, openapi_url=None)
    templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))
    app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")
    freshness = freshness_from_settings(settings)
    assets = [asset.symbol for asset in settings.watchlist.assets]

    def _filters(category: str | None, level: str | None, asset: str | None, show_c: int) -> dict[str, Any]:
        return {
            "category": category if category in ("news", "exchange") else None,
            "level": level if level in LEVELS else None,
            "asset": asset or None,
            "show_c": bool(show_c),
        }

    async def _rows(filters: dict[str, Any]) -> list[dict[str, Any]]:
        return await timeline(
            engine,
            freshness=freshness,
            category=filters["category"],
            level=filters["level"],
            asset=filters["asset"],
            include_c=filters["show_c"],
            limit=settings.web.timeline_limit,
        )

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request, category: str | None = None, level: str | None = None,
                    asset: str | None = None, show_c: int = 0):
        filters = _filters(category, level, asset, show_c)
        context = {"rows": await _rows(filters), "filters": filters, "assets": assets, "active": "home"}
        return templates.TemplateResponse(request, "index.html", context)

    @app.get("/api/timeline")
    async def api_timeline(category: str | None = None, level: str | None = None,
                           asset: str | None = None, show_c: int = 0):
        return await _rows(_filters(category, level, asset, show_c))

    @app.get("/fragment/cluster/{cluster_id}", response_class=HTMLResponse)
    async def cluster_fragment(request: Request, cluster_id: int):
        rows = await cluster_rows(engine, [cluster_id], freshness=freshness)
        if not rows:
            raise HTTPException(status_code=404, detail="事件簇不存在")
        return templates.TemplateResponse(request, "_row.html", {"row": rows[0]})

    @app.get("/status", response_class=HTMLResponse)
    async def status_page(request: Request):
        context = {
            "collectors": await collector_status(engine),
            "stats": await push_stats(engine, clock()),
            "active": "status",
        }
        return templates.TemplateResponse(request, "status.html", context)

    @app.get("/api/status")
    async def api_status():
        return {"collectors": await collector_status(engine), "push_stats": await push_stats(engine, clock())}

    @app.get("/stream")
    async def stream():
        return EventSourceResponse(broadcaster.subscribe(), ping=15)

    return app
```

- [ ] **Step 4: 写页面模板**

`tidewatch/web/templates/base.html`：
```html
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{% block title %}观潮{% endblock %}</title>
  <link rel="stylesheet" href="/static/style.css">
</head>
<body>
  <header class="top">
    <a class="brand" href="/">🌊 观潮</a>
    <nav>
      <a href="/" class="{{ 'on' if active == 'home' else '' }}">时间线</a>
      <a href="/status" class="{{ 'on' if active == 'status' else '' }}">状态</a>
    </nav>
    <span id="live" class="live off" title="实时连接状态">● 实时</span>
  </header>
  <main>{% block content %}{% endblock %}</main>
  <script src="/static/app.js"></script>
</body>
</html>
```

`tidewatch/web/templates/index.html`：
```html
{% extends "base.html" %}
{% block content %}
<form class="filters" method="get" action="/">
  <select name="category">
    <option value="">全部类别</option>
    <option value="news" {% if filters.category == 'news' %}selected{% endif %}>快讯</option>
    <option value="exchange" {% if filters.category == 'exchange' %}selected{% endif %}>交易所公告</option>
  </select>
  <select name="level">
    <option value="">全部等级</option>
    {% for l in ['S', 'A', 'B', 'C'] %}
    <option value="{{ l }}" {% if filters.level == l %}selected{% endif %}>{{ l }} 级</option>
    {% endfor %}
  </select>
  <select name="asset">
    <option value="">全部资产</option>
    {% for a in assets %}
    <option value="{{ a }}" {% if filters.asset == a %}selected{% endif %}>{{ a }}</option>
    {% endfor %}
  </select>
  <label><input type="checkbox" name="show_c" value="1" {% if filters.show_c %}checked{% endif %}> 显示 C 级</label>
  <button type="submit">筛选</button>
</form>
<table id="timeline" class="grid"
       data-category="{{ filters.category or '' }}" data-level="{{ filters.level or '' }}"
       data-asset="{{ filters.asset or '' }}" data-show-c="{{ 1 if filters.show_c else 0 }}">
  <thead><tr><th>时间</th><th>等级</th><th>内容</th><th>来源</th><th>资产</th></tr></thead>
  <tbody>
    {% for row in rows %}{% include "_row.html" %}{% else %}
    <tr class="empty"><td colspan="5">暂无事件</td></tr>
    {% endfor %}
  </tbody>
</table>
{% endblock %}
```

`tidewatch/web/templates/_row.html`：
```html
<tr id="c{{ row.cluster_id }}" class="lv-{{ row.level }}" data-ts="{{ row.occurred_at }}">
  <td class="time">{{ row.time_local }}</td>
  <td>
    <span class="badge lv-{{ row.level }}">{{ row.level }}</span>
    {% if row.pushed %}<span class="tag">已推送</span>{% endif %}
    {% if row.backfill %}<span class="tag muted">补抓</span>{% endif %}
  </td>
  <td class="title">{% if row.url %}<a href="{{ row.url }}" target="_blank" rel="noopener">{{ row.title }}</a>{% else %}{{ row.title }}{% endif %}</td>
  <td>{{ row.source_name }}{% if row.source_count > 1 %} <span class="tag">来源 +{{ row.source_count - 1 }}</span>{% endif %}</td>
  <td>{{ row.assets | join('、') }}</td>
</tr>
```

`tidewatch/web/templates/status.html`：
```html
{% extends "base.html" %}
{% block title %}状态 · 观潮{% endblock %}
{% block content %}
<h2>采集器</h2>
<table id="collectors" class="grid">
  <thead><tr><th>名称</th><th>状态</th><th>模式</th><th>最后成功</th><th>最后事件</th><th>连续失败</th><th>最近错误</th></tr></thead>
  <tbody>
    {% for c in collectors %}
    <tr id="s-{{ c.name }}" class="st-{{ c.status }}">
      <td>{{ c.source_name }}</td>
      <td class="status">{{ c.status_text }}</td>
      <td class="mode">{{ c.mode_text }}</td>
      <td>{{ c.last_success }}</td>
      <td>{{ c.last_event }}</td>
      <td class="failures">{{ c.consecutive_failures }}</td>
      <td class="error">{{ c.last_error }}</td>
    </tr>
    {% else %}
    <tr class="empty"><td colspan="7">尚无采集器运行记录</td></tr>
    {% endfor %}
  </tbody>
</table>
<h2>今日推送</h2>
<ul class="stats">
  <li>已送达 <b>{{ stats.ok }}</b></li>
  <li>演练 <b>{{ stats.dry_run }}</b></li>
  <li>待发送 <b>{{ stats.pending }}</b></li>
  <li>失败 <b>{{ stats.failed }}</b></li>
  <li>过期 <b>{{ stats.expired }}</b></li>
  <li>已取代 <b>{{ stats.superseded }}</b></li>
</ul>
{% endblock %}
```

- [ ] **Step 5: 写前端脚本与样式**

`tidewatch/web/static/app.js`：
```javascript
// 观潮前端：订阅 SSE，实时更新时间线与状态页
(function () {
  const live = document.getElementById("live");
  const source = new EventSource("/stream");
  source.onopen = () => live && live.classList.remove("off");
  source.onerror = () => live && live.classList.add("off");

  // 时间线：收到簇更新后，按当前筛选条件决定插入、替换或移除
  const table = document.getElementById("timeline");
  function matches(row) {
    const f = table.dataset;
    if (f.category && row.category !== f.category) return false;
    if (f.level && row.level !== f.level) return false;
    if (!f.level && f.showC !== "1" && row.level === "C") return false;
    if (f.asset && !row.assets.includes(f.asset)) return false;
    return true;
  }
  if (table) {
    source.addEventListener("cluster.updated", async (e) => {
      const row = JSON.parse(e.data);
      const existing = document.getElementById("c" + row.cluster_id);
      if (!matches(row)) {
        if (existing) existing.remove();
        return;
      }
      const resp = await fetch("/fragment/cluster/" + row.cluster_id);
      if (!resp.ok) return;
      const holder = document.createElement("tbody");
      holder.innerHTML = (await resp.text()).trim();
      const fresh = holder.firstElementChild;
      if (!fresh) return;
      fresh.classList.add("flash");
      if (existing) {
        existing.replaceWith(fresh);
        return;
      }
      const body = table.tBodies[0];
      const empty = body.querySelector("tr.empty");
      if (empty) empty.remove();
      // 按发生时间倒序插入
      const before = Array.from(body.rows).find((r) => r.dataset.ts < row.occurred_at);
      body.insertBefore(fresh, before || null);
    });
  }

  // 状态页：采集器状态变化
  const collectors = document.getElementById("collectors");
  if (collectors) {
    const text = { normal: "正常", abnormal: "异常", unknown: "未知" };
    source.addEventListener("health.changed", (e) => {
      const s = JSON.parse(e.data);
      const tr = document.getElementById("s-" + s.name);
      if (!tr) return;
      tr.className = "st-" + s.status;
      tr.querySelector(".status").textContent = text[s.status] || s.status;
      tr.querySelector(".mode").textContent = s.mode === "degraded" ? "降级" : "正常";
      tr.querySelector(".failures").textContent = s.consecutive_failures;
      tr.querySelector(".error").textContent = s.last_error || "";
    });
  }
})();
```

`tidewatch/web/static/style.css`：
```css
/* 观潮页面样式：跟随系统深浅色 */
:root { --bg: #f6f7f9; --card: #fff; --text: #1d1f23; --muted: #8a8f98; --line: #e3e5e8;
        --s: #e5484d; --a: #f59e0b; --b: #3b82f6; --c: #9ca3af; }
@media (prefers-color-scheme: dark) {
  :root { --bg: #111317; --card: #1a1d22; --text: #e6e8eb; --muted: #7d828b; --line: #2a2e35; }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text);
       font: 14px/1.6 -apple-system, "PingFang SC", "Helvetica Neue", sans-serif; }
.top { display: flex; align-items: center; gap: 20px; padding: 10px 20px; background: var(--card);
       border-bottom: 1px solid var(--line); position: sticky; top: 0; }
.brand { font-weight: 700; font-size: 16px; color: var(--text); text-decoration: none; }
nav a { color: var(--muted); text-decoration: none; margin-right: 14px; }
nav a.on { color: var(--text); font-weight: 600; }
.live { margin-left: auto; color: #22c55e; font-size: 12px; }
.live.off { color: var(--muted); }
main { padding: 16px 20px; max-width: 1200px; margin: 0 auto; }
.filters { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 12px; }
.filters select, .filters button { padding: 4px 8px; border: 1px solid var(--line); border-radius: 6px;
                                   background: var(--card); color: var(--text); }
.grid { width: 100%; border-collapse: collapse; background: var(--card); border-radius: 8px; overflow: hidden; }
.grid th, .grid td { padding: 8px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
.grid th { color: var(--muted); font-weight: 500; font-size: 12px; }
.time { white-space: nowrap; color: var(--muted); font-variant-numeric: tabular-nums; }
.title a { color: var(--text); text-decoration: none; }
.title a:hover { text-decoration: underline; }
.badge { display: inline-block; min-width: 22px; text-align: center; border-radius: 4px; color: #fff;
         font-weight: 700; font-size: 12px; padding: 0 5px; }
.badge.lv-S { background: var(--s); } .badge.lv-A { background: var(--a); }
.badge.lv-B { background: var(--b); } .badge.lv-C { background: var(--c); }
tr.lv-S td { background: color-mix(in srgb, var(--s) 8%, transparent); }
.tag { display: inline-block; font-size: 11px; padding: 0 6px; margin-left: 4px; border-radius: 10px;
       border: 1px solid var(--line); color: var(--muted); }
.tag.muted { border-style: dashed; }
tr.flash td { animation: flash 2s ease-out; }
@keyframes flash { from { background: color-mix(in srgb, var(--a) 30%, transparent); } to { background: transparent; } }
tr.empty td { text-align: center; color: var(--muted); }
tr.st-abnormal td { color: var(--s); }
.stats { list-style: none; padding: 0; display: flex; gap: 20px; }
.stats b { font-size: 18px; }
```

- [ ] **Step 6: 运行测试**

Run: `pytest tests/test_web_server.py -v`
Expected: `3 passed`

- [ ] **Step 7: 提交**

```bash
git add tidewatch/web tests/test_web_server.py
git commit -m "feat: 网站时间线、状态页与实时刷新" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

## Chunk 11: 装配、运维与验收

### Task 27: 程序装配、命令行与优雅退出

**Files:**
- Create: `tidewatch/app.py`
- Create: `tidewatch/__main__.py`
- Test: `tests/test_app.py`

装配顺序（系统设计 6.3、9.2、12）：先注册监听器（`EventSink` → `LiveUpdates`、`Dispatcher`；`HealthMonitor` → `LiveUpdates`），再 `health.load()`、`sink.start()`、`dispatcher.start()`，最后启动采集器。停止顺序：先取消采集器，再停 `Dispatcher` 与 `EventSink`（处理完已提交的命令），最后关闭网站、HTTP 客户端、通知器与数据库连接。非演练模式下若未配置飞书 webhook，拒绝启动并给出中文提示；只启动 `web` 组时不需要飞书。uvicorn 嵌入同一事件循环，信号由应用统一处理。

- [ ] **Step 1: 写失败的测试 `tests/test_app.py`**

```python
import pytest

from tidewatch.app import Application, parse_args
from tidewatch.collectors.binance_ann import BinanceAnnPoll, BinanceAnnStream
from tidewatch.collectors.jin10 import Jin10Collector
from tidewatch.collectors.okx_ann import OkxAnnCollector
from tidewatch.config import ConfigError
from tidewatch.notify.dryrun import DryRunNotifier


def test_parse_args():
    assert parse_args([]).only is None and parse_args([]).dry_run is False
    args = parse_args(["--only", "web", "--dry-run"])
    assert args.only == ["web"] and args.dry_run is True
    with pytest.raises(SystemExit):
        parse_args(["--only", "unknown"])


async def test_requires_feishu_unless_dry_run(settings):
    with pytest.raises(ConfigError) as exc:
        Application(settings, groups={"news"}, dry_run=False)
    assert "FEISHU_WEBHOOK" in str(exc.value)
    web_only = Application(settings, groups={"web"}, dry_run=False)  # 只开网站不需要飞书
    assert isinstance(web_only.notifier, DryRunNotifier) and web_only.collectors == []
    await web_only.stop()


async def test_collectors_and_binance_mode(settings):
    app = Application(settings, groups={"news"}, dry_run=True)
    assert [type(c) for c in app.collectors] == [Jin10Collector, OkxAnnCollector, BinanceAnnPoll]
    assert app.binance_degraded is True
    await app.stop()
    settings.secrets.binance_api_key, settings.secrets.binance_api_secret = "k", "s"
    app = Application(settings, groups={"news"}, dry_run=True)
    assert isinstance(app.collectors[-1], BinanceAnnStream) and app.binance_degraded is False
    await app.stop()


async def test_start_and_stop_cleanly(engine, settings):
    app = Application(settings, groups={"news"}, dry_run=True)
    for collector in app.collectors:  # 测试中不访问外部接口
        await collector.client.aclose()
    app.collectors = []
    await app.start()
    await app.stop()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_app.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tidewatch.app'`

- [ ] **Step 3: 实现 `tidewatch/app.py`**

```python
"""组件装配、启动与优雅退出"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from pathlib import Path

import uvicorn

from tidewatch.clock import Clock, utcnow
from tidewatch.collectors.base import Collector
from tidewatch.collectors.binance_ann import BinanceAnnPoll, BinanceAnnStream
from tidewatch.collectors.http import make_client
from tidewatch.collectors.jin10 import Jin10Collector
from tidewatch.collectors.okx_ann import OkxAnnCollector
from tidewatch.config import ConfigError, Settings, load_settings
from tidewatch.health import HealthMonitor
from tidewatch.logging_setup import setup_logging
from tidewatch.notify.base import Notifier
from tidewatch.notify.dispatcher import Dispatcher
from tidewatch.notify.dryrun import DryRunNotifier
from tidewatch.notify.feishu import FeishuNotifier
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.db import create_db_engine
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.live import LiveUpdates
from tidewatch.web.server import create_app

logger = logging.getLogger(__name__)

GROUPS = ("news", "web")
REPO_ROOT = Path(__file__).resolve().parents[1]
STOP_TIMEOUT_S = 8


class _EmbeddedServer(uvicorn.Server):
    """嵌入同一事件循环的 uvicorn：不接管信号，由应用统一处理"""

    @contextlib.contextmanager
    def capture_signals(self):
        yield

    def install_signal_handlers(self) -> None:  # 兼容旧版 uvicorn
        pass


class Application:
    def __init__(self, settings: Settings, *, groups: set[str], dry_run: bool, clock: Clock = utcnow) -> None:
        self.settings = settings
        self.groups = set(groups)
        self.dry_run = dry_run
        self.clock = clock
        self.notifier = self._make_notifier()
        self.engine = create_db_engine(settings.secrets.database_url)
        self.broadcaster = Broadcaster()
        self.health = HealthMonitor(self.engine, self.notifier, settings.dispatch, clock=clock)
        self.sink = create_event_sink(self.engine, settings, clock=clock)
        self.dispatcher = Dispatcher(self.engine, self.sink, self.notifier, settings.dispatch, clock=clock)
        self.live = LiveUpdates(self.engine, self.broadcaster, settings)
        self.collectors: list[Collector] = self._make_collectors() if "news" in self.groups else []
        self._tasks: list[asyncio.Task] = []
        self._collector_tasks: list[asyncio.Task] = []
        self._server: _EmbeddedServer | None = None

    # ---------- 装配 ----------

    def _make_notifier(self) -> Notifier:
        if self.dry_run or "news" not in self.groups:
            return DryRunNotifier()
        webhook = self.settings.secrets.feishu_webhook
        if not webhook:
            raise ConfigError(
                "未配置飞书 webhook：请在 .env 中设置 FEISHU_WEBHOOK（建议同时设置 FEISHU_SECRET），或使用 --dry-run 演练"
            )
        return FeishuNotifier(webhook, self.settings.secrets.feishu_secret)

    def _make_collectors(self) -> list[Collector]:
        settings, secrets = self.settings, self.settings.secrets
        common = {"sink": self.sink, "health": self.health, "engine": self.engine, "clock": self.clock}

        def client_for(name: str):
            return make_client(settings.collector(name).proxy, settings.proxy_url)

        collectors: list[Collector] = []
        if settings.collector("jin10").enabled:
            collectors.append(Jin10Collector(settings.collector("jin10"), client=client_for("jin10"), **common))
        if settings.collector("okx_ann").enabled:
            collectors.append(OkxAnnCollector(settings.collector("okx_ann"), client=client_for("okx_ann"), **common))
        binance_cfg = settings.collector("binance_ann")
        if binance_cfg.enabled:
            if secrets.binance_api_key and secrets.binance_api_secret:
                collectors.append(
                    BinanceAnnStream(
                        binance_cfg,
                        api_key=secrets.binance_api_key,
                        api_secret=secrets.binance_api_secret,
                        ws_proxy=settings.proxy_url if binance_cfg.proxy == "global" else None,
                        client=client_for("binance_ann"),
                        **common,
                    )
                )
            else:
                collectors.append(BinanceAnnPoll(binance_cfg, client=client_for("binance_ann"), **common))
        for collector in collectors:
            self.health.register(collector.name, collector.cfg.stale_after_s)
        return collectors

    @property
    def binance_degraded(self) -> bool:
        return any(isinstance(c, BinanceAnnPoll) for c in self.collectors)

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        if "news" in self.groups:
            self.sink.on_ingest(self.live.on_ingest)
            self.health.on_change(self.live.on_health)
            self.dispatcher.attach()
            await self.health.load()
            await self.sink.start()
            await self.dispatcher.start()
            if any(c.name == "binance_ann" for c in self.collectors):
                # 每次启动都按实际情况设置，补上 API Key 后状态页会恢复「正常」
                await self.health.set_mode("binance_ann", "degraded" if self.binance_degraded else "normal")
                if self.binance_degraded:
                    logger.warning("未配置币安只读 API Key，币安公告以降级模式（轮询列表接口）运行")
            self._tasks += [
                asyncio.create_task(self.health.run(), name="health"),
                asyncio.create_task(self.live.run(), name="live"),
            ]
            self._collector_tasks = [
                asyncio.create_task(c.run(), name=f"collector-{c.name}") for c in self.collectors
            ]
            for task in self._collector_tasks:
                task.add_done_callback(_log_collector_exit)
        if "web" in self.groups:
            web = self.settings.web
            config = uvicorn.Config(
                create_app(self.engine, self.broadcaster, self.settings, clock=self.clock),
                host=web.host,
                port=web.port,
                log_config=None,
                access_log=False,
                timeout_graceful_shutdown=3,  # 浏览器保持 SSE 连接时也能按时退出
            )
            self._server = _EmbeddedServer(config)
            self._tasks.append(asyncio.create_task(self._server.serve(), name="web"))
            logger.info("网站已启动：http://%s:%s", web.host, web.port)
        mode = "，演练模式（不发送飞书）" if self.dry_run else ""
        logger.info("观潮已启动：%s%s", "、".join(sorted(self.groups)), mode)

    async def stop(self) -> None:
        """优雅退出：先停采集，再处理完已提交的写入与推送，最后关闭网站与各类连接"""
        for task in self._collector_tasks:
            task.cancel()
        await asyncio.gather(*self._collector_tasks, return_exceptions=True)
        await self.dispatcher.stop()
        await self.sink.stop()
        if self._server is not None:
            # 信号由应用接管，需主动通知 uvicorn 退出（会一并关闭 SSE 连接）
            self._server.handle_exit(signal.SIGTERM, None)
        for task in self._tasks:
            if task.get_name() != "web":
                task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for collector in self.collectors:
            if collector.client is not None:
                await collector.client.aclose()
        await self.notifier.aclose()
        await self.engine.dispose()
        logger.info("观潮已停止")


def _log_collector_exit(task: asyncio.Task) -> None:
    """采集器任务意外结束时记录错误（正常情况下只会被取消）"""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("采集器任务 %s 异常退出：%s", task.get_name(), exc, exc_info=exc)
    else:
        logger.error("采集器任务 %s 意外结束", task.get_name())


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="tidewatch", description="观潮：个人市场监控系统")
    parser.add_argument("--only", nargs="+", choices=GROUPS, help="只启动指定组件组（默认全部）")
    parser.add_argument("--dry-run", action="store_true", help="演练模式：不发送飞书，只写日志")
    return parser.parse_args(argv)


async def run(settings: Settings, *, groups: set[str], dry_run: bool) -> None:
    app = Application(settings, groups=groups, dry_run=dry_run)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await app.start()
    try:
        await stop_event.wait()
    finally:
        logger.info("收到停止信号，正在退出…")
        try:
            await asyncio.wait_for(app.stop(), timeout=STOP_TIMEOUT_S)
        except asyncio.TimeoutError:
            logger.error("退出超时（%s 秒），强制结束", STOP_TIMEOUT_S)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        settings = load_settings(REPO_ROOT)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2
    setup_logging(settings.log, REPO_ROOT)
    try:
        asyncio.run(run(settings, groups=set(args.only or GROUPS), dry_run=args.dry_run))
    except ConfigError as exc:
        logger.error("%s", exc)
        return 2
    return 0
```

`tidewatch/__main__.py`：
```python
"""命令行入口：python -m tidewatch [--only news web] [--dry-run]"""

from tidewatch.app import main

if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: 运行测试与全量回归**

Run: `pytest tests/test_app.py -v && pytest -q`
Expected: `4 passed`；全量回归全部通过（0 failed）

- [ ] **Step 5: 本地试运行（演练模式）**

Run: `python -m tidewatch --dry-run`（前台运行，约 1 分钟后按 Ctrl+C）
Expected:
- 日志出现「观潮已启动：news、web，演练模式（不发送飞书）」「网站已启动：http://127.0.0.1:8000」；未配置币安 API Key 时出现「降级模式」警告
- 浏览器打开 `http://127.0.0.1:8000` 能看到金十快讯与公告；`/status` 中三个采集器状态为「正常」，币安为「降级」
- Ctrl+C 后 10 秒内打印「观潮已停止」并退出
- `log/tidewatch_<今天日期>.log` 存在且为中文日志

- [ ] **Step 6: 提交**

```bash
git add tidewatch/app.py tidewatch/__main__.py tests/test_app.py
git commit -m "feat: 程序装配、命令行与优雅退出" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 28: launchd 守护与使用说明

**Files:**
- Create: `deploy/com.captain.tidewatch.plist`（模板，`__REPO__` 由安装脚本替换）
- Create: `deploy/install.sh`、`deploy/uninstall.sh`
- Create: `README.md`

- [ ] **Step 1: 写 `deploy/com.captain.tidewatch.plist`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- 观潮 launchd 配置模板：由 deploy/install.sh 替换仓库路径与启动参数占位符后安装 -->
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.captain.tidewatch</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/caffeinate</string>
    <string>-i</string>
    <string>__REPO__/.venv/bin/python</string>
    <string>-m</string>
    <string>tidewatch</string>
    __ARGS__
  </array>
  <key>WorkingDirectory</key>
  <string>__REPO__</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>StandardOutPath</key>
  <string>/dev/null</string>
  <key>StandardErrorPath</key>
  <string>__REPO__/log/launchd.err.log</string>
</dict>
</plist>
```

- [ ] **Step 2: 写 `deploy/install.sh`**

```bash
#!/usr/bin/env bash
# 安装观潮 launchd 服务：开机自启、崩溃自动重启、运行期间阻止空闲睡眠
# 用法：deploy/install.sh [--dry-run]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.captain.tidewatch"
TARGET="$HOME/Library/LaunchAgents/$LABEL.plist"
ARGS=""
if [[ "${1:-}" == "--dry-run" ]]; then
  ARGS="<string>--dry-run</string>"
fi

mkdir -p "$REPO/log" "$HOME/Library/LaunchAgents"
sed -e "s#__REPO__#$REPO#g" -e "s#__ARGS__#$ARGS#g" "$REPO/deploy/$LABEL.plist" > "$TARGET"
plutil -lint "$TARGET"

# 已安装则先卸载，等旧进程完全退出（最长约 20 秒）后再加载
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
for _ in $(seq 1 20); do
  launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || break
  sleep 1
done
launchctl bootstrap "gui/$(id -u)" "$TARGET"
echo "已安装并启动：$LABEL"
echo "查看状态：launchctl print gui/$(id -u)/$LABEL | head -20"
echo "查看日志：tail -f $REPO/log/tidewatch_\$(TZ=Asia/Shanghai date +%F).log"
```

- [ ] **Step 3: 写 `deploy/uninstall.sh`**

```bash
#!/usr/bin/env bash
# 卸载观潮 launchd 服务
set -euo pipefail

LABEL="com.captain.tidewatch"
TARGET="$HOME/Library/LaunchAgents/$LABEL.plist"
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
rm -f "$TARGET"
echo "已停止并卸载：$LABEL"
```

Run: `chmod +x deploy/install.sh deploy/uninstall.sh`

- [ ] **Step 4: 校验模板替换结果（不安装）**

Run:
```bash
sed -e "s#__REPO__#$PWD#g" -e "s#__ARGS__#<string>--dry-run</string>#g" deploy/com.captain.tidewatch.plist > /tmp/tidewatch-check.plist && plutil -lint /tmp/tidewatch-check.plist && rm /tmp/tidewatch-check.plist
```
Expected: `/tmp/tidewatch-check.plist: OK`

- [ ] **Step 5: 写 `README.md`**

````markdown
# 观潮（Tidewatch）

个人使用的市场监控系统：采集金十快讯与币安、OKX 交易所公告，按 S/A/B/C 分级，重要事件推送到飞书群，并在本机网站实时展示。完整设计见 `docs/plans/2026-09-30-tidewatch-design.md`。

## 环境

- macOS、Python 3.11、PostgreSQL 18（本机）
- 访问海外接口需要代理（在 `config.local.yaml` 设置 `proxy_url`）

## 安装

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env                                # 填写数据库连接、飞书 webhook 等
cp config.local.example.yaml config.local.yaml      # 填写代理、个人关注资产
alembic upgrade head
```

`.env` 与 `config.local.yaml` 已被 `.gitignore` 排除，不要提交。

## 运行

```bash
python -m tidewatch --dry-run        # 演练：不发送飞书，只写日志
python -m tidewatch                  # 正式运行（需要 FEISHU_WEBHOOK）
python -m tidewatch --only web       # 只启动网站
```

网站：http://127.0.0.1:8000（时间线）、http://127.0.0.1:8000/status（采集器状态与当日推送统计）

## 开机自启（launchd）

```bash
deploy/install.sh            # 正式运行
deploy/install.sh --dry-run  # 演练模式常驻
deploy/uninstall.sh          # 卸载
```

注意：服务通过 `caffeinate -i` 阻止空闲睡眠，但**合上笔记本盖子仍会睡眠**，需要常开或外接显示器。

## 配置要点

- 阈值、关键词、频率：`config.yaml`（可在 `config.local.yaml` 按键覆盖；列表项按 `symbol`/`id` 合并，写 `remove: true` 删除默认项）
- 币安公告：`.env` 中配置只读的 `BINANCE_API_KEY` 与 `BINANCE_API_SECRET` 时使用 WebSocket 实时推送，否则每 30 秒轮询（状态页显示「降级」）
- 日志：`log/tidewatch_YYYY-MM-DD.log`，保留 14 天

## 测试

```bash
pytest -q
```

测试库默认使用 `.env` 中数据库连接对应的 `tidewatch_test` 库。
````

- [ ] **Step 6: 提交**

```bash
git add deploy README.md
git commit -m "chore: launchd 守护脚本与使用说明" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 29: 验收（与用户一起完成）

**Files:**
- Modify: `openspec/changes/add-phase1-core-pipeline/tasks.md`（勾选第 10 组验收项）

本任务需要用户参与（提供飞书机器人、观察手机通知），执行者每完成一步向用户报告结果。

- [ ] **Step 1: 全量测试**

Run: `pytest -q`
Expected: 全部通过（0 failed），逐一对照 `openspec/changes/add-phase1-core-pipeline/specs/*/spec.md` 中的场景确认均有测试覆盖；未覆盖的场景列出并补测试。

- [ ] **Step 2: 飞书联调**

请用户在飞书群添加「自定义机器人」、开启签名校验，把 webhook 与密钥写入 `.env` 的 `FEISHU_WEBHOOK`、`FEISHU_SECRET`。然后运行：

```bash
python - <<'EOF'
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from tidewatch.config import load_settings
from tidewatch.notify.base import EventCard
from tidewatch.notify.feishu import FeishuNotifier

async def main():
    s = load_settings(Path("."))
    n = FeishuNotifier(s.secrets.feishu_webhook, s.secrets.feishu_secret)
    card = EventCard(push_id=0, cluster_id=0, level="S", title="【联调】S 级测试消息", source="jin10",
                     occurred_at=datetime.now(timezone.utc), url="https://flash.jin10.com", assets=["BTC"],
                     body="这是一条联调消息", source_count=2, upgraded=False)
    print(await n.send_event(card, mention_all=True))
    card.level, card.title, card.upgraded = "A", "【联调】A 级升级测试", True
    print(await n.send_event(card, mention_all=False))
    print(await n.send_digest([card, card]))
    print(await n.send_health("jin10", False, "【联调】连续失败 5 次"))
    print(await n.send_health("jin10", True, "【联调】恢复正常"))
    await n.aclose()

asyncio.run(main())
EOF
```
Expected: 5 行 `SendResult(ok=True, ...)`；飞书群依次收到红色 S 卡片（@所有人）、橙色「升级」A 卡片、A 级汇总、红色异常告警、绿色恢复通知。

- [ ] **Step 3: 演练常驻 24 小时**

Run: `deploy/install.sh --dry-run`，期间手动重启两次：`launchctl kickstart -k gui/$(id -u)/com.captain.tidewatch`
24 小时后检查：
```bash
psql "$(grep ^TIDEWATCH_DATABASE_URL= .env | cut -d= -f2- | sed 's/+asyncpg//')" <<'SQL'
-- 各等级事件数量（用于评估阈值是否合适）
SELECT source, final_level, count(*) FROM events GROUP BY 1, 2 ORDER BY 1, 2;
-- 推送状态（演练模式应为 dry_run / superseded / expired，不应有长期 pending 或 failed）
SELECT status, count(*) FROM push_log GROUP BY 1;
-- 同一簇同一等级不应被推送两次（重启不重复推送）
SELECT cluster_id, level, count(*) FROM push_log WHERE status IN ('dry_run', 'ok') GROUP BY 1, 2 HAVING count(*) > 1;
SQL
grep -c "ERROR" log/tidewatch_$(TZ=Asia/Shanghai date +%F).log
```
Expected: 进程持续运行（`launchctl print` 显示 `state = running`）；最后一个查询无结果；ERROR 行数为 0 或均可解释。把各等级数量与用户一起评估，必要时调整 `config.local.yaml` 中的关键词与阈值。

- [ ] **Step 4: 健康告警演练**

在 `config.local.yaml` 把 `proxy_url` 临时改成不可用地址（如 `http://127.0.0.1:9`）并重启服务。
Expected: 约 5 分钟内状态页中 OKX、币安公告变为「异常」，日志出现「【演练】健康通知」；恢复 `proxy_url` 并重启后变回「正常」，日志出现恢复通知。

- [ ] **Step 5: 正式上线与延迟测量**

Run: `deploy/install.sh`（正式模式，需已配置飞书），运行至少 2 小时后：
```bash
psql "$(grep ^TIDEWATCH_DATABASE_URL= .env | cut -d= -f2- | sed 's/+asyncpg//')" -c "
SELECT p.level, count(*), round(avg(extract(epoch FROM p.sent_at - e.collected_at))::numeric, 1) AS avg_s,
       max(extract(epoch FROM p.sent_at - e.collected_at)) AS max_s
FROM push_log p JOIN events e ON e.id = p.event_id
WHERE p.status = 'ok' AND p.batch_id IS NULL GROUP BY p.level;"
```
Expected: 单独推送（非汇总）的 S/A 平均延迟 ≤ 10 秒。

- [ ] **Step 6: 更新 OpenSpec 并提交**

在 `openspec/changes/add-phase1-core-pipeline/tasks.md` 勾选第 10 组，运行 `openspec validate add-phase1-core-pipeline --strict` 确认通过。
```bash
git add openspec/changes/add-phase1-core-pipeline/tasks.md
git commit -m "docs: 一期验收完成" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
之后按用户的全局约定执行 OpenSpec 归档（`/opsx:archive`），把 delta 规范合并到 `openspec/specs/`。
