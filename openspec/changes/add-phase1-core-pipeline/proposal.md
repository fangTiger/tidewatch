## Why

观潮（Tidewatch）需要先有一条可靠的「采集 → 分级 → 推送 → 展示」全链路，后续的 Hyperliquid 巨鲸、机构动向、Arkham 链上告警、AI 解读都要接在这条链路上。一期用最简单、最有价值的两类数据源（金十快讯、币安/OKX 交易所公告）把底座跑通并验证稳定性，让用户尽早拿到一个可用的「快讯雷达」。

完整系统设计见 `docs/plans/2026-09-30-tidewatch-design.md`（下称「系统设计」）。

## What Changes

- 新建 Python 项目骨架（包名 `tidewatch`），入口 `python -m tidewatch`，支持 `--only` 与 `--dry-run`
- 配置分层：公开的 `config.yaml`、私有的 `config.local.yaml`、密钥 `.env`
- PostgreSQL 表与 Alembic 迁移：`events`、`event_clusters`、`collector_state`、`push_log`
- 统一事件模型 `Event` 与唯一写入入口 `EventSink`：同一事务内完成落库去重、规则分级、聚簇、推送决策与游标推进
- 规则引擎（异步接口，带只读历史上下文），一期实现 `news` 与 `exchange` 两类规则
- 事件簇与以簇为单位的推送决策（新鲜度、升级补推）
- 推送状态机、防刷屏（S 级永不合并，A 级超限每 60 秒汇总）、飞书自定义机器人通知器与演练通知器
- 采集器契约（轮询型 / 长连接型基类，限流、退避、游标、健康上报）
- 三个采集器：金十快讯 `jin10`、币安公告 `binance_ann`（WS + 列表接口补缺）、OKX 公告 `okx_ann`
- 网站：首页事件时间线（SSE 实时推送）、状态页
- 运维：采集器健康监控与飞书健康告警、按天轮转的中文日志、launchd 开机自启与崩溃重启

## Capabilities

### New Capabilities

- `event-pipeline`: 统一事件模型、`EventSink` 事务化处理流程、去重、规则引擎与分级、事件簇、以簇为单位的推送决策与新鲜度
- `notification`: 推送状态机、失败重试与期限、防刷屏与汇总、飞书通知器、演练通知器、健康告警通道
- `news-sources`: 采集器契约与基类（限流、退避、游标、健康判定），金十快讯、币安公告、OKX 公告三个采集器及其 `news` / `exchange` 分级规则
- `web-dashboard`: 首页事件时间线（按类别/等级/资产筛选，SSE 实时更新）与状态页
- `runtime-ops`: 配置分层与校验、命令行参数（`--only`、`--dry-run`）、日志、launchd 守护、启动恢复

### Modified Capabilities

（无，本变更为项目首个变更）

## Impact

- **新增代码**：`tidewatch/` 包（`config`、`models`、`app`、`collectors/`、`pipeline/`、`notify/`、`web/`、`store/`）、`migrations/`、`deploy/com.captain.tidewatch.plist`、`tests/`
- **新增依赖**：`httpx`、`websockets`、`fastapi`、`uvicorn`、`sse-starlette`、`jinja2`、`sqlalchemy[asyncio]`、`asyncpg`、`alembic`、`pydantic`、`pyyaml`、`python-dotenv`；测试 `pytest`、`pytest-asyncio`、`respx`
- **外部系统**：本机 PostgreSQL（库 `tidewatch`、测试库 `tidewatch_test`）；飞书群自定义机器人；金十站内快讯接口；币安公告 WS（需只读 API Key）与公告列表接口；OKX 公开公告接口
- **不在本期范围**：Hyperliquid（二期）；SEC、ETF、Arkham、经济日历、AI（三期）；联动卡片与帖子草稿（四期）
