## ADDED Requirements

### Requirement: Hyperliquid 组件组
系统 SHALL 将 `hyperliquid` 加入 `python -m tidewatch --only <组…>` 的可选组。不带 `--only` 时 MUST 随现有默认组一起启动；选择 `--only hyperliquid` 时 MUST 自动启动数据库、`EventSink`、规则、推送调度器和健康监控，但不得启动新闻采集器或网站；选择 `--only hyperliquid web` 时 MUST 同时启用 SSE 实时更新。任何产生事件的组（`news` 或 `hyperliquid`）都 MUST 触发同一流水线与推送组件，不能继续把它们仅绑定到 `news`。

#### Scenario: 只启动 Hyperliquid
- **WHEN** 用户执行 `python -m tidewatch --only hyperliquid --dry-run`
- **THEN** Hyperliquid 子系统、统一流水线、演练推送和健康监控启动，新闻采集器与网站不启动

#### Scenario: Hyperliquid 与网站组合
- **WHEN** 用户执行 `python -m tidewatch --only hyperliquid web --dry-run`
- **THEN** 巨鲸监控和网站启动，Hyperliquid 事件及状态可通过 SSE 更新，新闻采集器不启动

#### Scenario: Hyperliquid 单组使用正式通知器
- **WHEN** 用户执行 `python -m tidewatch --only hyperliquid` 且已配置飞书 webhook
- **THEN** 系统使用 `FeishuNotifier` 发送 Hyperliquid 的 S/A 事件，不因未选择 `news` 组而退化为演练通知器

#### Scenario: 一期组保持兼容
- **WHEN** 用户执行 `python -m tidewatch --only news --dry-run`
- **THEN** 启动行为与一期一致，不要求 Hyperliquid 配置中的可选核心钱包非空

### Requirement: Hyperliquid 关键任务守护
系统 SHALL 将 REST 调度、成交流 WS、核心钱包 WS、排行榜调度、分层重算、画像批处理、行情消费及必要的状态写入循环纳入 `Application.critical_tasks()`。任一长期关键任务未被取消却异常退出或正常返回时，主进程 MUST 以非零状态退出，以便 launchd 重启；优雅退出时 MUST 先停止接收新消息，再排空已提交的 `EventSink` 命令并关闭连接。

#### Scenario: 成交流任务意外退出
- **WHEN** `hl_trades_ws` 长期任务在应用运行中意外返回
- **THEN** 应用记录中文错误、执行有界清理并以非零状态退出

#### Scenario: 优雅退出不留半事务
- **WHEN** 应用收到 SIGTERM 且 EventSink 正在处理一批 Hyperliquid 事件
- **THEN** 系统停止新采集、等待当前命令提交或回滚，再关闭 REST/WS 与数据库连接

### Requirement: 五个健康子源
系统 SHALL 通过一期 `HealthMonitor.register/success/failure` 分别管理 `hl_rest`、`hl_trades_ws`、`hl_core_ws`、`hl_leaderboard`、`hl_profile`。各子源 MUST 独立累计失败、持久化到 `collector_state`、触发每源每小时最多一次的健康告警并在恢复时通知。画像与排行榜的 `stale_after` MUST 按低频周期设置，实时源按请求或心跳周期设置。

#### Scenario: 子源告警互不连带
- **WHEN** `hl_profile` 连续批次失败达到异常条件，而其他四个子源持续成功
- **THEN** 仅 `hl_profile` 进入异常并发送受一期限流保护的健康告警

#### Scenario: 低频任务未到期
- **WHEN** 排行榜上次成功距今 11 小时且周期为 12 小时
- **THEN** `hl_leaderboard` 不因没有分钟级心跳而判为异常

### Requirement: Hyperliquid 本地数据忽略
系统 SHALL 将仓库根目录 `data/` 加入 `.gitignore`，使 `data/labels/` 的 Hypurrscan 别名缓存和其他运行期本地数据不被 Git 跟踪。实现所需的目录 SHALL 在运行时按需创建；测试夹具 MUST 继续存放在受版本控制的 `tests/fixtures/hyperliquid/`，不得放入 `data/`。

#### Scenario: 标签缓存不被跟踪
- **WHEN** 系统在 `data/labels/` 写入别名缓存
- **THEN** Git 不把该文件列为未跟踪文件，而 `tests/fixtures/hyperliquid/` 下的测试夹具仍可被跟踪

### Requirement: 中文运行日志与限流诊断
系统 SHALL 沿用一期中文日志，记录 Hyperliquid 子源、钱包短地址、请求种类、权重、队列优先级、退避时长和健康状态，但 MUST NOT 记录密钥或完整私有配置。发生 429 时 MUST 能从日志区分单次限流与持续 5 分钟的持续 429，并说明当前代理模式；切换代理节点的提示 MUST 说明出口 IP 会变化。

#### Scenario: 429 日志可诊断
- **WHEN** `hl_rest` 收到一次 429 并进入 2 秒退避
- **THEN** 中文日志包含子源、请求类型、权重、退避时长与 `global`/`direct` 模式，不包含任何密钥
