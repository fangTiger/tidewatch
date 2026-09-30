## ADDED Requirements

### Requirement: 统一事件模型
系统 SHALL 将所有来源的数据转换为统一的 `Event` 并持久化到 `events` 表，字段至少包括：`source`、`category`、`event_type`、`occurred_at`、`ref_date`、`collected_at`、`title`、`body`（截断到 2,000 字）、`url`、`assets`、`amount_usd`、`rule_level`、`rule_hits`、`ai_status`、`final_level`、`dedupe_key`、`cluster_id`、`raw`。所有时间字段 MUST 以 UTC `timestamptz` 存储。

#### Scenario: 采集事件落库
- **WHEN** 采集器产出一条带时区的事件并提交给 `EventSink`
- **THEN** `events` 表新增一行，`occurred_at` 与 `collected_at` 以 UTC 存储，`body` 长度不超过 2,000 字

### Requirement: 确定性去重与幂等写入
系统 SHALL 以 `dedupe_key` 唯一索引保证同一条源数据只入库一次；重复写入 MUST 被静默忽略，且被忽略的事件 MUST NOT 进入分级、聚簇与推送。

#### Scenario: 重复事件被忽略
- **WHEN** 同一 `dedupe_key` 的事件第二次提交给 `EventSink`
- **THEN** `events` 表行数不变，且不产生新的推送记录

#### Scenario: 重启后重复采集
- **WHEN** 进程重启后采集器重新拉取到已入库的数据
- **THEN** 这些数据不会重复入库，也不会重复推送

### Requirement: 事务化处理流程
系统 SHALL 通过唯一的串行入口 `EventSink` 处理每批采集结果，并在同一个数据库事务内依次完成：插入去重、规则分级、聚簇、推送决策、AI 标记、写入采集器游标。`ingest` MUST 在事务提交后才返回。

#### Scenario: 事务提交失败
- **WHEN** 一批事件在提交前发生数据库错误
- **THEN** 该批事件与游标均未写入，采集器保持旧游标并在退避后重试

#### Scenario: 单条事件处理异常
- **WHEN** 一批中某条事件在分级或聚簇时抛出异常
- **THEN** 系统回滚到该事件的保存点，将其记为 `rule_level = final_level = C`、`rule_hits = ["error"]` 并新建单事件簇，同批其余事件与游标正常提交，并写错误日志

### Requirement: 异步规则引擎
系统 SHALL 提供异步规则引擎 `RuleEngine.evaluate(event, ctx) -> RuleResult(level, hits)`，其中 `ctx: RuleContext` 提供只读的历史查询；阈值与关键词 MUST 来自配置。关键词规则 MUST 支持「全部包含（all）」「任一包含（any）」「排除（none）」组合。在 AI 未启用时，`final_level` MUST 等于 `rule_level`。

#### Scenario: 关键词组合命中
- **WHEN** 规则配置为 `{all: [关税], any: [中国, 全面, 对等]}` 且事件标题为「美国宣布对中国加征关税」
- **THEN** 该规则命中，其规则 ID 出现在 `rule_hits` 中

#### Scenario: 排除词生效
- **WHEN** 事件标题命中 `all` 与 `any` 条件，但同时命中 `none` 中的词
- **THEN** 该规则不命中

### Requirement: 事件聚簇
系统 SHALL 将同一件事的多条事件归入同一个事件簇（`event_clusters`），聚簇候选 MUST 从数据库按类别与窗口查询，重启后无需恢复内存状态。`news` 类在 10 分钟窗口内按规范化标题的字符二元组 Jaccard 相似度 ≥ 0.5 匹配（阈值可配置）；若双方 `assets` 均非空还 MUST 有交集。`news` 与 `exchange` 之间 MUST 使用显式跨类规则匹配：`news` 标题同时命中交易所名（币安、Binance、OKX、欧易）与上币或下架类关键词，且标题中出现该 `exchange` 事件涉及的任一币种代码。

#### Scenario: 相似快讯归为同簇
- **WHEN** 10 分钟内先后入库两条标题高度相似、资产相同的金十快讯
- **THEN** 两条事件的 `cluster_id` 相同，簇的 `member_count` 为 2

#### Scenario: 超出窗口不聚簇
- **WHEN** 与簇首条事件相似的新快讯在簇 `first_seen_at` 之后 11 分钟入库
- **THEN** 新快讯进入一个新簇

#### Scenario: 快讯与交易所公告跨类匹配
- **WHEN** 币安公告「Binance Will List XYZ」入库后 5 分钟内，金十快讯「币安将上线 XYZ」入库，两者资产均为 XYZ
- **THEN** 两条事件归入同一个簇，不论谁先到

### Requirement: 以簇为单位的推送决策
系统 SHALL 在簇加入新事件或簇内事件等级变化时执行推送决策。设 `F` 为簇内新鲜事件的最高 `final_level`、`C` 为簇的 `committed_level`：当 `F ∈ {S, A}` 且 `C` 为空时 MUST 安排推送，代表事件为等级 `F` 的新鲜事件中最早的一条；当 `F = S` 且 `C = A` 时 MUST 安排 S 推送；其他情况 MUST NOT 推送。安排推送 MUST 与决策在同一事务中写入 `push_log(status=pending)` 并更新 `committed_level`。新鲜事件指 `now − occurred_at ≤ 该源 max_push_age` 的事件。

#### Scenario: 首次推送
- **WHEN** 一个新簇的首条事件为 A 级且新鲜
- **THEN** 系统写入一条 `pending` 推送记录，簇的 `committed_level` 变为 A

#### Scenario: B 级先到、S 级后到
- **WHEN** 簇内首条事件为 B 级，随后一条新鲜的 S 级事件加入该簇
- **THEN** 系统安排一次 S 推送，代表事件为该 S 级事件

#### Scenario: A 已送达后升级为 S
- **WHEN** 簇的 A 推送已送达（`delivered_level = A`），随后新鲜的 S 级事件加入
- **THEN** 系统安排一次标注「升级」的 S 推送

#### Scenario: A 未送达时升级为 S
- **WHEN** 簇的 A 推送仍为 `pending` 或 `failed`，随后新鲜的 S 级事件加入
- **THEN** 原 A 推送标记为 `superseded`，S 推送按首次推送样式安排，不标注「升级」

#### Scenario: 同级事件不重复推送
- **WHEN** 簇已安排 A 推送，又有一条 A 级事件加入同簇
- **THEN** 不产生新的推送记录，只更新簇的来源数与成员数

#### Scenario: 过期事件不推送
- **WHEN** 补抓到一条 `occurred_at` 早于 `max_push_age` 的 S 级事件
- **THEN** 事件照常入库与聚簇，但不产生推送记录

### Requirement: AI 标记
系统 SHALL 在事件处理时设置 `ai_status`：AI 已启用且 `final_level ≥ B` 时为 `pending`，否则为 `skipped`。一期 AI 未启用（`ai.enabled: false`），所有事件 MUST 为 `skipped`。

#### Scenario: 一期事件的 AI 状态
- **WHEN** `ai.enabled` 为 false 时入库一条 A 级事件
- **THEN** 该事件的 `ai_status` 为 `skipped`
