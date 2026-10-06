## ADDED Requirements

### Requirement: Hyperliquid 统一事件与确定性去重
系统 SHALL 把监控事件构造为一期 `Event`：`source = hl_monitor`、`category = hyperliquid`；新巨鲸事件使用 `source = hl_discovery`、`category = hyperliquid`。`occurred_at` MUST 取成交时间，或无成交证据时取触发比较的上游仓位快照时间。`dedupe_key` MUST 为 `hl:{规范地址}:{币种}:{事件类型}:{成交tid或持久化检测时间戳}`；反手拆分的两个事件因事件类型不同而拥有不同键。来源快照时间/发现窗口结束时间 MUST 在重试前持久化或从上游确定值派生，不能使用每次重试的本机当前时间。`hl_monitor` 与 `hl_discovery` 的 `max_push_age` MUST 均为 10 分钟。

#### Scenario: 同一成交重放不重复入库
- **WHEN** 带同一 `tid` 的核心钱包成交在 WS 重连后再次到达
- **THEN** 两次生成相同 `dedupe_key`，一期唯一索引只保留一条事件且不重复推送

#### Scenario: 无成交快照重试键稳定
- **WHEN** 同一份仓位快照比较因数据库提交失败而重试
- **THEN** 重试使用相同的上游快照时间生成相同 `dedupe_key`，而不是新的本机检测时刻

#### Scenario: 十分钟旧事件不推送
- **WHEN** 补抓到一条 `occurred_at` 距当前超过 10 分钟的 S 级 Hyperliquid 事件
- **THEN** 事件照常入库、分级与聚簇，但一期推送决策不安排推送

### Requirement: Hyperliquid 专用聚簇
系统 SHALL 向一期 `Clusterer` 增加 `hyperliquid` 策略，`cluster_key` 为（规范地址, 币种, 事件族），窗口从簇 `first_seen_at` 起 30 分钟。事件族 MUST 为：同向加码 `{hl_open, hl_increase}`、离场 `{hl_decrease, hl_close}`、强平风险 `{hl_liq_risk}`、被清算 `{hl_liquidated}`、新巨鲸 `{hl_new_whale}`。反手拆分的平仓与开仓 MUST 分别进入离场与同向加码簇。聚簇候选 MUST 从数据库查询，使重启后冷却继续有效。

#### Scenario: 连续同向加仓聚为一簇
- **WHEN** 同一地址同一币种在 30 分钟窗口内先开仓后加仓
- **THEN** 两个事件进入同一个同向加码簇，后续同级事件不重复推送

#### Scenario: 同钱包不同币种不聚簇
- **WHEN** 同一地址在 30 分钟内分别加仓 BTC 与 ETH
- **THEN** 两个事件进入不同簇

#### Scenario: 反手进入两个事件族
- **WHEN** 同一仓位反手并拆成 `hl_close` 与 `hl_open`
- **THEN** 平仓进入离场簇，开仓进入同向加码簇，可分别按等级作推送决策

#### Scenario: 超出三十分钟开启新簇
- **WHEN** 同一地址同一币种同一事件族的新事件发生在原簇 `first_seen_at` 之后 31 分钟
- **THEN** 新事件进入新簇

### Requirement: Hyperliquid 事件事务处理
系统 SHALL 通过一期 `EventSink.ingest` 在同一事务完成 Hyperliquid 事件插入去重、`hyperliquid` 规则分级、专用聚簇、推送决策、AI 标记与对应子源游标推进。二期 `ai.enabled` 仍为 false，Hyperliquid 事件的 `ai_status` MUST 为 `skipped`。事务提交前 MUST NOT 广播 SSE 或交给 Dispatcher。

#### Scenario: 事件与游标原子提交
- **WHEN** 一批仓位事件在推送决策阶段发生数据库错误
- **THEN** 一期保存点/事务语义生效，游标不会越过未提交数据，提交前也没有 SSE 或推送队列通知

#### Scenario: 二期不进入 AI 队列
- **WHEN** 一条 S 级 Hyperliquid 事件在二期入库
- **THEN** `ai_status` 为 `skipped`，事件仍按规则等级参与推送

### Requirement: 复用一期推送决策与防刷屏
系统 SHALL 对 Hyperliquid 簇复用一期 `PushDecider`、`Dispatcher`、失败重试、A 级防刷屏、S 级立即推送与 A→S 升级逻辑。安排推送仍 MUST 在事件事务内写 `push_log` 并更新 `committed_level`；发送结果仍 MUST 通过 `EventSink.push_result` 回写。

#### Scenario: 强平风险 A 升 S
- **WHEN** 强平风险簇的 A 推送已送达后，同簇在新鲜度内升为 S
- **THEN** Dispatcher 发送标注「升级」的 S 卡片并 @所有人

#### Scenario: 同级加仓不重复推送
- **WHEN** 同向加码簇已安排 A 推送，30 分钟窗口内又加入一条 A 级加仓
- **THEN** 不新增推送记录，仅更新簇成员、代表数据与网站展示

### Requirement: 巨鲸飞书卡片
系统 SHALL 向后兼容扩展一期 `EventCard` 与 `FeishuNotifier`，为 Hyperliquid 单条事件卡片展示：钱包备注/标签和短地址、币种、方向、数量、名义价值、开仓价、强平价与距强平、杠杆、盈亏、层级。缺失字段 MUST 省略而不是显示伪造的 0；所有上游或用户文本 MUST 继续使用安全的纯文本渲染。新闻/交易所卡片格式 MUST 保持兼容。

#### Scenario: 完整巨鲸卡片
- **WHEN** 一条核心钱包减仓事件同时有备注、成交数量、名义价值、开仓价、强平价、杠杆和已实现盈亏
- **THEN** 飞书卡片包含全部字段、核心层级和短地址，并沿用事件等级颜色与升级标记

#### Scenario: 缺少强平价
- **WHEN** 一条事件的上游状态没有强平价
- **THEN** 卡片省略强平价和距强平字段，不显示 0 或误导性百分比

#### Scenario: 高频钱包不进入发送队列
- **WHEN** 做市/高频钱包事件经规则降为 C
- **THEN** 事件可在网站查看，但一期推送决策不创建 `push_log`
