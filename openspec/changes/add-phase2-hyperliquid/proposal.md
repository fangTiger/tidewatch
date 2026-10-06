## Why

观潮一期已经建立可恢复的「采集 → 分级 → 聚簇 → 推送 → 展示」主链路，二期需要在不复制这套底座的前提下接入 Hyperliquid 官方公共 API，把巨鲸名单、实时仓位变化、成交流发现和钱包画像转化为可分级、可去重、可推送、可查询的统一事件。这样既能在核心钱包发生关键操作时提供秒级提醒，也能通过重点钱包对账、候选池与画像批处理扩大观察覆盖面。

完整系统设计见 `docs/plans/2026-09-30-tidewatch-design.md`（下称「系统设计」）。本变更承接尚在验收的一期变更 `add-phase1-core-pipeline`，复用其已实现接口并仅做向后兼容扩展。

## What Changes

- 新增 Hyperliquid REST 客户端与 WS 连接管理：监控与批处理预算分别为 600/分钟和 200/分钟，使用 10 秒突发窗口并空桶启动；全局 429 熔断按 1→300 秒退避且冷却后仅放行单个探测，AIMD 每次降速 50%、最低 120/分钟、每连续 60 秒无 429 恢复配置速率的 10%；重点钱包对账默认并发 4；每 50 秒 ping、自动重连并限制连接数与订阅数。默认总预算低于上游文档的 1,200/分钟，因为代理出口 IP 可能共享且上游实际额度不稳定
- 新增四张表 `hl_wallets`、`hl_positions`、`hl_fills`、`hl_state`，通过下接一期迁移的新 Alembic revision 建表
- 新增排行榜每 12 小时流式导入、地址标签缓存、候选/重点/核心分层，以及导入后和每天 08:00 的晋升淘汰重算
- 新增名单管理命令 `python -m tidewatch.tools.hl_wallets`，支持 `add`、`remove`、`core`、`exclude`、`label`、`list`；核心钱包也可由 `config.local.yaml` 提供，最多 10 个且允许为空
- 新增核心钱包 WS 实时监控与重点钱包 REST 对账，比较仓位快照检测开仓、加仓、减仓、平仓、反手、接近强平和被清算事件
- 新增主 dex 与活跃 HIP-3 dex 成交流发现：按吃单方、币种与 3 秒窗口聚合，发现单窗口达到 500 万美元且账户或仓位达标的新巨鲸
- 新增 `hyperliquid` 规则、30 分钟专用聚簇、10 分钟新鲜度与巨鲸飞书卡片；复用一期 `EventSink`、`RuleEngine`、`Dispatcher`、`FeishuNotifier` 和推送状态机
- 新增每晚钱包画像与成交增量入库，计算系统设计 7.4 的指标并标注做市/高频、波段、方向型风格
- 新增巨鲸列表页 `/whales`、钱包详情页 `/whales/{address}`、首页实时行情条与巨鲸多空净仓位侧栏，并通过现有 `/stream` SSE 增量更新
- 新增 `hyperliquid` 配置段与 `--only hyperliquid` 组件组；Hyperliquid 五个子源分别接入健康监控与关键任务守护
- 将 `data/` 加入 `.gitignore`，确保 Hypurrscan 标签缓存及本地名单相关数据不会进入公开仓库

## Capabilities

### New Capabilities

- `hyperliquid-client`: Hyperliquid REST/WS 客户端、权重预算、退避、代理、连接与订阅管理、专用表和迁移
- `whale-registry`: 排行榜导入、地址标签、名单管理、候选/重点/核心分层及晋升淘汰
- `whale-monitor`: 核心钱包与重点钱包监控、实时价格、仓位事件检测和 `hyperliquid` 分级规则
- `whale-discovery`: 全市场成交流聚合、陌生地址筛查、新巨鲸晋升与事件生成
- `wallet-profile`: 成交增量存储、每晚批量画像、画像指标与风格标签

### Modified Capabilities

- `event-pipeline`: 增加 Hyperliquid 事件的确定性去重、30 分钟聚簇、10 分钟新鲜度、推送决策与巨鲸卡片数据
- `web-dashboard`: 增加巨鲸页、钱包详情、首页行情条、净仓位侧栏、Hyperliquid 状态与 SSE 增量
- `runtime-ops`: 增加 `hyperliquid` 配置、组件组、关键任务守护、五个健康子源与 `data/` 忽略规则

由于一期变更尚未归档且 `openspec/specs/` 为空，上述一期能力的 delta 在本变更中使用 `ADDED Requirements` 表达新增行为，不引用不存在的正式基线。

## Impact

- **新增代码范围（后续实现阶段）**：`tidewatch/hyperliquid/`、`tidewatch/tools/hl_wallets.py`、Hyperliquid 规则、巨鲸网页模板与查询、`tests/fixtures/hyperliquid/` 及相应测试
- **扩展的一期接口（后续实现阶段）**：`TokenBucket` 的加权获取、`RuleEngine` 类别注册、`Clusterer` 的 Hyperliquid 策略、`EventCard` 的可选结构化详情、来源新鲜度查询、SSE 事件、应用组件组与关键任务集合
- **数据库**：新增一期迁移之后的独立 Alembic revision；不修改一期迁移文件
- **配置与本地状态**：`config.yaml` 新增公开默认 `hyperliquid` 段；私有核心钱包与代理覆盖写入 `config.local.yaml`；标签缓存写入被忽略的 `data/labels/`
- **外部系统**：仅访问 Hyperliquid 官方 REST/WS、排行榜数据端点与待确认可用性的 Hypurrscan 别名接口；不引入付费源
- **兼容性**：一期新闻、公告、推送、网站和健康行为保持不变；Hyperliquid 子系统不继承 `Collector`，但复用 `EventSink` 与 `HealthMonitor`
- **Non-Goals**：AI 解读与今日要点、机构数据、联动卡片、帖子草稿、手机访问、自动交易和跟单
