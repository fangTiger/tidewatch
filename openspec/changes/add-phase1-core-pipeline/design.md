## Context

- 项目从零开始，仓库 `fangTiger/tidewatch`（公开）。完整系统设计见 `docs/plans/2026-09-30-tidewatch-design.md`（下称「系统设计」），本文件只记录一期落地时的技术决策。
- 运行环境：用户本机 macOS，Python 3.11，本地 PostgreSQL 18（`localhost:5432`，库 `tidewatch`，测试库 `tidewatch_test`，均已创建），海外访问经代理 `http://127.0.0.1:10808`。
- 一期范围：底座全链路 + 金十快讯、币安公告、OKX 公告三个采集器 + 首页时间线与状态页 + launchd。

## Goals / Non-Goals

**Goals:**
- 事件从采集到推送全程可恢复：重启后不重复入库、不漏数据、推送至多重复一次
- 未触发防刷屏时，S/A 事件在入库后 10 秒内送达飞书
- 为二期（Hyperliquid）、三期（机构 + AI）预留稳定接口：`Event`、`EventSink`、`RuleEngine`/`RuleContext`、`Collector`、`Notifier`
- `--dry-run` 可连续运行 24 小时用于调阈值

**Non-Goals:**
- Hyperliquid、SEC、ETF、Arkham、经济日历、AI 解读（后续各期）
- 手机访问与登录（后续按需通过 Tailscale + 访问令牌）
- 多进程部署（`--only` 只用于调试与将来拆分）

## Decisions

### D1 虚拟环境与打包：`.venv` + `pyproject.toml`（hatchling）
本机没有 `uv`，用标准 `python3 -m venv .venv` + `pip install -e ".[dev]"`，零额外工具。备选 poetry / uv：需额外安装，收益不大。

### D2 数据访问：SQLAlchemy 2 Core（异步，asyncpg 驱动），不用 ORM
流水线大量依赖 `INSERT … ON CONFLICT DO NOTHING RETURNING`、`SAVEPOINT`、行级更新，Core 表达更直接、行为可预期。表定义集中在 `store/tables.py`，Alembic 基于同一份 `MetaData` 生成迁移。备选 ORM：会话与对象状态管理在串行事务模型下是额外负担。

### D3 串行 `EventSink`：单个 asyncio 工作协程 + 命令队列
`ingest` / `push_result`（一期）以命令形式进入 `asyncio.Queue`，由唯一工作协程按序执行；调用方 `await` 命令完成（事务提交后才返回）。事件量为每秒个位数，串行消除了聚簇与推送决策的竞争。每条事件的分级、聚簇、推送决策包在 `begin_nested()` 保存点中，异常时降级为 C 级单事件簇，整批事务继续。

### D4 标题相似度：字符二元组 Jaccard，阈值 0.5
规范化 = NFKC（全角转半角）、英文小写、去空白与标点；取字符二元组集合计算 Jaccard 相似度，≥ 0.5 视为同一件事。实测（2026-09-30）：近乎重复的快讯 0.85–1.0，改写过的同一事件 0.3–0.4，无关快讯 0–0.09。10 分钟窗口内的候选簇只有十几个，直接逐一比对即可，不需要存储指纹。备选 64 位 SimHash（字符 3-gram）：实测两条只差两个字的标题汉明距离即达 9，按设计阈值 3 只能识别完全重复，已放弃；备选 `jieba` 分词：依赖重、对短标题收益有限。候选簇从数据库按「类别 + 窗口」查询，重启无需内存状态。

### D5 跨类匹配（金十快讯 ↔ 交易所公告）用显式规则
中英文标题无法用相似度比对。规则：10 分钟窗口内，`news` 事件标题同时命中交易所名（币安、Binance、OKX、欧易）与上币/下架类关键词，且标题中出现该 `exchange` 事件涉及的任一币种代码，即归为同簇（系统设计 6.4）。

### D6 推送决策以 `committed_level` 为准，发送与决策解耦
决策在 `ingest` 事务内写 `push_log(pending)` 并更新 `committed_level`；提交后由 `Dispatcher` 发送。`Dispatcher` 维护发送队列、A 级 5 分钟滑动窗口与汇总缓冲；发送结果通过 `EventSink.push_result` 串行回写，更新 `push_log` 状态与 `delivered_level`。启动时加载 `pending`/`failed` 记录继续发送（系统设计 6.7、6.8、9.2）。

### D7 网站：FastAPI + Jinja2 + 原生 JS + `sse-starlette`
进程内广播器（每个 SSE 连接一个 `asyncio.Queue`）接收 `event.created`、`cluster.updated`、`health.changed`；时间线页面首屏服务端渲染最近 200 个簇，之后增量追加。uvicorn 以 `Server(config).serve()` 方式嵌入同一事件循环。备选前端框架：需要构建工具，违背「改 HTML 刷新即生效」。

### D8 飞书自定义机器人签名
按飞书规则：`string_to_sign = f"{timestamp}\n{secret}"`，以其为 HMAC-SHA256 的密钥对空消息签名后 Base64；消息体用 `interactive` 卡片。实现时以飞书开放平台文档为准并用真实机器人联调。

### D9 币安公告：WS 为主、列表接口补缺；无 API Key 时降级为纯轮询
WS（`com_announcement_en`）需要只读 API Key 与 Secret 做 HMAC-SHA256 签名。若 `.env` 未提供，`binance_ann` 自动退化为每 30 秒轮询列表接口，并在状态页注明「降级模式」。两种来源用同一去重键算法 `sha1(规范化标题 + 发布日期)`。

### D10 金十接口参数参照开源实现，并用录制数据固化
金十为非公开接口；实现时参照 RSSHub / newsnow 当前源码确定请求地址与请求头，把真实响应录制到 `tests/fixtures/jin10/` 作为解析测试基准。接口变更由健康监控发现。

### D11 时间与时区
库内一律 UTC `timestamptz`；页面与日志显示 `Asia/Shanghai`；各源时间解析为带时区的 `datetime` 后入库。

### D12 测试
`pytest` + `pytest-asyncio`；数据库相关测试连 `tidewatch_test`（每个测试在事务中执行并回滚，或按表清空）；HTTP 用 `respx` 模拟；币安 WS 用本地 `websockets.serve` 假服务器；飞书用 `respx` 断言请求体与签名。

## Risks / Trade-offs

- [金十接口变更或加签名] → 健康告警提醒；解析与请求分离，修复只改一个文件；备用源作为插件预留
- [币安 WS 签名或订阅协议与文档不符] → 列表接口补缺兜底（每 10 分钟，降级模式每 30 秒），不影响可用性
- [单进程串行 `EventSink` 成为瓶颈] → 一期事件量极低；二期接入 Hyperliquid 前复测吞吐，必要时批量化
- [「至少一次」推送可能重复一次] → 仅在「已发送、未回写」的崩溃窗口出现，接受
- [Mac 合盖睡眠] → `caffeinate -i` 只阻止空闲睡眠；在 README 中提示用户不要合盖
- [字符相似度识别不了改写过的同一新闻（实测 0.3–0.4，低于阈值）] → 宁可重复推送也不误合并；一期只有金十一个新闻源，影响小；三期可借助 AI 判断语义重复

## Migration Plan

1. 创建 `.venv`，安装依赖；复制 `.env.example` → `.env`、`config.local.example.yaml` → `config.local.yaml` 并填写
2. `alembic upgrade head` 在 `tidewatch` 库建表
3. `python -m tidewatch --dry-run` 实跑 24 小时，观察状态页与日志，调整阈值
4. 去掉 `--dry-run`，安装 launchd（`deploy/` 下提供安装/卸载脚本）
5. 回滚：`launchctl unload` 停止服务；数据库可 `alembic downgrade base`

## Open Questions

- 飞书群机器人 webhook 与签名密钥（用户提供后才能联调，开发期以演练通知器与 `respx` 测试替代）
- 币安只读 API Key/Secret 是否提供（未提供则走 D9 降级模式）
- 关注资产清单：先用系统设计 6.9 的默认值，用户可在 `config.local.yaml` 调整
