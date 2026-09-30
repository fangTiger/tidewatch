## 1. 项目骨架与配置

- [x] 1.1 创建 `.venv` 与 `pyproject.toml`（hatchling，依赖与 `dev` 可选依赖），建立 `tidewatch/` 包与 `tests/` 目录结构
- [x] 1.2 编写 `config.yaml`（默认值）、`config.local.example.yaml`、`.env.example`
- [x] 1.3 实现 `tidewatch/config.py`：三层加载、深度合并、pydantic 校验与中文错误信息（含测试）
- [x] 1.4 实现日志初始化：`log/tidewatch_YYYY-MM-DD.log` 按天轮转保留 14 天，同时输出到标准输出

## 2. 存储

- [x] 2.1 在 `store/tables.py` 定义 `events`、`event_clusters`、`collector_state`、`push_log` 表与索引
- [x] 2.2 配置 Alembic 并生成一期初始迁移，验证 `upgrade head` 与 `downgrade base`
- [x] 2.3 实现 `store/db.py` 异步引擎与连接池，测试夹具连接 `tidewatch_test` 并在每个测试后清理数据

## 3. 领域模型与规范化

- [x] 3.1 实现 `models.py`：`Event`、`FetchResult`、`RuleResult`、等级枚举与比较
- [x] 3.2 实现 `pipeline/normalize.py`：标题规范化、关注资产别名识别、字符二元组 Jaccard 标题相似度（含测试）

## 4. 规则引擎

- [x] 4.1 实现 `RuleEngine`（异步）、`RuleContext` 与关键词组合匹配（all / any / none）（含测试）
- [x] 4.2 实现 `news` 规则：金十 S/A/B/C 分级（含边界测试）
- [x] 4.3 实现 `exchange` 规则：公告类型 × 交易所 × 是否关注资产的分级表（含测试）

## 5. 事件流水线

- [x] 5.1 实现 `pipeline/cluster.py`：`news` 标题相似度聚簇、`news`↔`exchange` 跨类显式匹配、窗口判断，候选从数据库查询（含测试）
- [x] 5.2 实现 `pipeline/decision.py` 推送决策：新鲜事件判定、首次推送、A→S 升级与取代、同级不重复（含测试）
- [x] 5.3 实现 `pipeline/sink.py`：串行命令队列，`ingest` 事务（插入去重、分级、聚簇、推送决策、AI 标记、游标）与逐条保存点降级（含测试）
- [x] 5.4 实现 `EventSink.push_result`：回写推送状态、更新 `delivered_level`、处理已取代记录（含测试）

## 6. 推送

- [x] 6.1 定义 `Notifier` 接口与 `EventCard`，实现 `DryRunNotifier`
- [x] 6.2 实现 `FeishuNotifier`：签名、事件卡片（等级着色、@所有人、升级标注）、汇总消息、健康告警（用 `respx` 测试请求体与签名）
- [x] 6.3 实现推送调度器：发送队列、S 级立即发送、A 级 5 分钟滑动窗口与 60 秒汇总缓冲、`batch_id`（含测试）
- [x] 6.4 实现失败重试（立即 3 次 + 后台每 60 秒）、重试期限与 `expired`、启动时加载 `pending`/`failed` 记录（含测试）

## 7. 采集器

- [x] 7.1 实现 `collectors/base.py`：`PollCollector`、`StreamCollector`、令牌桶限流、指数退避、提交后推进游标、健康上报与异常判定（含测试）
- [x] 7.2 实现健康监控：状态变化广播、`send_health` 异常告警每源每小时 1 条、恢复通知（含测试）
- [ ] 7.3 实现 `jin10` 采集器：参照开源实现确定接口，录制真实响应到 `tests/fixtures/jin10/`，解析与去重键测试
- [ ] 7.4 实现 `okx_ann` 采集器：接口解析、公告分类、录制夹具与测试
- [ ] 7.5 实现 `binance_ann` 采集器：WS 签名订阅与心跳、列表接口补缺（重连后 + 每 10 分钟）、无 API Key 降级轮询、公告分类；用本地假 WS 服务器测试 WS 与列表键一致

## 8. 网站

- [ ] 8.1 实现 SSE 广播器与 `/stream` 端点（事务提交后发出消息）
- [ ] 8.2 实现首页时间线：簇合并展示、代表事件选择、筛选（类别/等级/资产，默认隐藏 C 级）、「补抓」标记、SSE 增量更新
- [ ] 8.3 实现状态页：采集器状态、降级模式、最近错误、当天推送统计，SSE 实时更新

## 9. 装配与运维

- [ ] 9.1 实现 `app.py` 与 `__main__.py`：按组启动组件、`--only`、`--dry-run`、uvicorn 嵌入同一事件循环、启动恢复、SIGTERM/SIGINT 优雅退出
- [ ] 9.2 编写 `deploy/com.captain.tidewatch.plist` 与安装/卸载脚本（`caffeinate -i`、`RunAtLoad`、`KeepAlive`）
- [ ] 9.3 编写 `README.md`（中文）：安装、配置、运行、launchd、注意事项（不要合盖）

## 10. 验收

- [ ] 10.1 全部测试通过（`pytest`），覆盖规范中的每个场景
- [ ] 10.2 使用真实飞书机器人联调：S/A 卡片、汇总消息、健康告警各发一次
- [ ] 10.3 `--dry-run` 连续运行 24 小时：无崩溃；期间重启 2 次，确认无重复入库、无漏数据
- [ ] 10.4 断开某个源（如关闭代理）时状态页标红并发出健康告警，恢复后发出恢复通知
- [ ] 10.5 测量未触发防刷屏时 S/A 事件从入库到飞书送达的延迟，确认 ≤ 10 秒
