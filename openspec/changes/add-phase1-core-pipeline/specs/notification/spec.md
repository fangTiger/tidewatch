## ADDED Requirements

### Requirement: 推送状态机
系统 SHALL 以 `push_log` 记录每个簇的每次推送，状态 MUST 为 `pending`、`ok`、`failed`、`expired`、`superseded`、`dry_run` 之一。`ok` 与 `dry_run` MUST 视为已送达并更新簇的 `delivered_level`。发送结果 MUST 通过 `EventSink.push_result` 串行回写。

#### Scenario: 发送成功
- **WHEN** 调度器成功发送一条 `pending` 推送
- **THEN** 该记录变为 `ok`，簇的 `delivered_level` 更新为该推送的等级

#### Scenario: 演练模式视为已送达
- **WHEN** 以 `--dry-run` 运行并处理一条 `pending` 推送
- **THEN** 不发出任何 HTTP 请求，记录变为 `dry_run`，簇的 `delivered_level` 被更新，重启后不再发送

### Requirement: 失败重试与期限
系统 SHALL 在发送失败时立即退避重试 3 次；仍失败则标为 `failed`，并由后台每 60 秒重试一次。重试期限 MUST 为「代表事件新鲜度截止时间」与「记录创建后 24 小时」中较早者；超过期限 MUST 标为 `expired`。

#### Scenario: 重试后成功
- **WHEN** 首次发送失败、第二次立即重试成功
- **THEN** 记录状态为 `ok`

#### Scenario: 超过期限放弃
- **WHEN** 一条 `failed` 记录持续失败直到超过重试期限
- **THEN** 记录变为 `expired`，并在状态页的过期计数中体现

### Requirement: 启动恢复
系统 SHALL 在启动时加载 `push_log` 中 `pending` 与 `failed` 且未过重试期限的记录继续发送。推送语义 MUST 为「至少一次」：发送成功但回写前崩溃时，重启后允许重复发送一次。

#### Scenario: 崩溃后继续发送
- **WHEN** 进程在一条推送为 `pending` 时被杀死并重启
- **THEN** 该推送在重启后被发送并变为 `ok`

### Requirement: 取代处理
系统 SHALL 在推送记录被标记为 `superseded` 后，把它从发送队列与汇总缓冲中移除；若该记录在被标记前已发出，回写时 MUST 只更新簇的 `delivered_level`，状态保持 `superseded`。

#### Scenario: 被取代的 A 推送不再发送
- **WHEN** 一条 A 推送在汇总缓冲中等待时被 S 推送取代
- **THEN** 下一条汇总消息不包含该 A 推送

### Requirement: 防刷屏
系统 SHALL 立即、单独发送 S 级推送，S 级 MUST NOT 参与合并。A 级在 5 分钟滑动窗口内最多单独发送 10 条；超出部分 MUST 进入汇总缓冲，缓冲非空时每 60 秒发送一条汇总消息，汇总中的每个簇在 `push_log` 各占一行并以同一个 `batch_id` 关联。

#### Scenario: A 级超限进入汇总
- **WHEN** 5 分钟内产生 13 条 A 级推送
- **THEN** 前 10 条单独发送，后 3 条在 60 秒内合并为一条汇总消息发送，3 条记录共享同一个 `batch_id`

#### Scenario: S 级不受限
- **WHEN** A 级已达到单独发送上限时产生一条 S 级推送
- **THEN** S 级推送立即单独发送

### Requirement: 飞书通知器
系统 SHALL 通过飞书群自定义机器人 webhook 发送消息，并按飞书规则计算签名。事件卡片 MUST 包含时间（Asia/Shanghai）、来源、标题、关键数字与原文链接，标题栏按等级着色（S 红、A 橙）；S 级 MUST @所有人；升级推送 MUST 标注「升级」。webhook 地址与签名密钥 MUST 从 `.env` 读取。

#### Scenario: S 级卡片
- **WHEN** 发送一条 S 级事件推送
- **THEN** 请求体为带签名的卡片消息，标题栏为红色，并包含 @所有人

### Requirement: 健康告警通道
系统 SHALL 通过 `Notifier.send_health` 发送采集器健康告警，与事件推送分开限流：每个采集器每小时最多 1 条异常告警；采集器恢复正常时 MUST 发送一条恢复通知。

#### Scenario: 健康告警限流
- **WHEN** 同一采集器在 1 小时内两次进入异常状态
- **THEN** 只发送 1 条异常告警
