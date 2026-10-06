## ADDED Requirements

### Requirement: 核心钱包实时订阅
系统 SHALL 为最多 10 个核心钱包订阅 `userFills` 与 `allDexsClearinghouseState`，将成交与约每 5 秒到达的状态更新规范化并持久化。WS 断线重连后 MUST 恢复订阅，并立即对全部热点钱包执行一次 REST 仓位对账。

#### Scenario: 核心状态产生仓位事件
- **WHEN** 核心钱包的 `allDexsClearinghouseState` 显示某币仓位从 0 变为达到门槛的非 0 值
- **THEN** 系统比较前后快照并将 `hl_open` 事件交给一期 `EventSink`

#### Scenario: 重连后补最终状态
- **WHEN** 核心 WS 断线期间漏掉成交并随后重连
- **THEN** 系统恢复原订阅并立即 REST 对账热点钱包，以当前仓位快照发现最终状态变化

### Requirement: 重点钱包优先级对账
系统 SHALL 对重点钱包执行 REST 对账：热点钱包每 20 秒、其余每 3 分钟；重点钱包出现在成交流时去抖 2–3 秒后发起查询。预算不足时 MUST 按「成交触发 > 接近强平钱包对账 > 其他热点对账 > 普通对账」出队，低优先级任务 MUST 顺延而不得丢弃。

#### Scenario: 成交触发优先
- **WHEN** 队列同时存在普通对账、热点对账和成交触发查询且当前预算只够一个请求
- **THEN** 成交触发查询先出队，其他任务保留等待后续预算

#### Scenario: 成交去抖合并
- **WHEN** 同一重点钱包在 2 秒内连续出现在多笔成交中
- **THEN** 系统在 2–3 秒去抖窗口结束后只安排一次该钱包状态查询

#### Scenario: 普通钱包刷新上限
- **WHEN** 一个非热点重点钱包持续没有成交触发
- **THEN** 系统仍至少每 3 分钟安排一次对账，使仓位变化在 5 分钟内可被检测

### Requirement: 热点钱包判定
系统 SHALL 把「任一仓位距强平不超过 10%」或「任一仓位名义不少于 10,000,000 美元」的重点钱包视为热点；热点最多 30 个，按最近强平距离由近到远、再按最大仓位名义由大到小选择。

#### Scenario: 热点超过三十个
- **WHEN** 35 个重点钱包满足热点条件
- **THEN** 系统仅将排序最靠前的 30 个按 20 秒频率对账，其余仍按普通重点钱包频率对账

### Requirement: 多 dex 与统一账户状态
系统 SHALL 根据 `userAbstraction` 识别统一账户/组合保证金；其账户价值 MUST 等于 `clearinghouseState` 权益加 `spotClearinghouseState` 的 USDC。主 dex 以外的 HIP-3 仓位 MUST 按 dex 参数分别查询。当前仓位 MUST 幂等保存到以（地址, dex, 币种）为键的 `hl_positions`。

#### Scenario: 统一账户权益
- **WHEN** 统一账户合约权益为 800,000 美元且现货 USDC 为 300,000 美元
- **THEN** 系统计算账户价值为 1,100,000 美元

#### Scenario: HIP-3 仓位不被主 dex 覆盖
- **WHEN** 同一钱包在主 dex 和一个 HIP-3 dex 持有同名币种仓位
- **THEN** 两个快照按不同 dex 分别保存和比较

### Requirement: 成交补查与增量存储
系统 SHALL 在重点钱包仓位变化后查询从上次游标起的 `userFillsByTime`，同钱包相邻查询至少间隔 60 秒；未找到对应成交时再查询 `userTwapSliceFills`。名单内成交 MUST 幂等写入 `hl_fills`，并更新 `hl_state` 游标；TWAP 的全零 hash MUST NOT 被用作唯一去重依据。

#### Scenario: 成交查询受一分钟间隔保护
- **WHEN** 同一钱包在首次成交查询后 30 秒再次发生仓位变化
- **THEN** 第二次查询被合并或顺延到最小间隔满足，仓位事件本身不被丢弃

#### Scenario: 普通成交缺失时查询 TWAP
- **WHEN** 仓位快照发生变化但 `userFillsByTime` 没有返回对应成交
- **THEN** 系统查询 `userTwapSliceFills` 并用其补充事件与成交记录

### Requirement: 钱包监控基线
系统 SHALL 在 `hl_state` 为每个重点或核心钱包持久化独立监控基线。首次 REST 对账或首次核心状态消息 MUST 只保存当前仓位、初始化强平滞回状态和成交补查游标，不得生成开仓、加仓、减仓、平仓、反手或接近强平事件。钱包从候选或排除状态进入重点/核心层时 MUST 使旧监控基线失效。分层重算写入的 `hl_positions` 快照 MUST NOT 自动视为监控基线。

#### Scenario: 首次观察已有大量仓位
- **WHEN** 钱包尚无监控基线且首次状态包含多个已有仓位
- **THEN** 系统保存快照并建立基线，但不生成任何仓位或接近强平事件

#### Scenario: 基线后发生开仓
- **WHEN** 钱包已建立监控基线且后续仓位从 0 变为达到门槛的非 0 值
- **THEN** 系统按正常差分生成一条 `hl_open` 事件

#### Scenario: 晋升后重新建立基线
- **WHEN** 钱包从候选或排除状态恢复并进入重点或核心层
- **THEN** 系统使旧基线与旧补查游标失效，下一次观察只建立新基线

### Requirement: 补抓事件时效
系统 SHALL 以监控基线时刻初始化无游标钱包的 `userFillsByTime.startTime`，并忽略早于基线的普通与 TWAP 成交。被清算事件 MUST 只来自基线之后的成交。已有基线钱包的上一快照早于当前时间减 `max_push_age` 时，若没有成交证据提供确切时间，快照差分事件的 `occurred_at` MUST 取上一快照时间，且 `raw.backfill` MUST 为 `true`，使事件只入库不推送；有成交证据时 MUST 使用成交时间。

#### Scenario: 无游标时不回扫基线前历史
- **WHEN** 钱包已有监控基线但成交补查游标缺失
- **THEN** 首次 `userFillsByTime` 从基线时刻开始，本次普通与 TWAP 补查返回的基线前成交不写入 `hl_fills` 且不生成历史清算事件

#### Scenario: 停机后无成交证据的变化
- **WHEN** 上一快照距当前 30 分钟且对账发现仓位变化，但补查没有找到对应成交
- **THEN** 事件以旧快照时间作为 `occurred_at`、标记 `raw.backfill=true`，并且不产生 `push_log`

### Requirement: 仓位开平与反手检测
系统 SHALL 比较相邻仓位快照：`szi` 从 0 到非 0 且新仓名义达到币种门槛时生成 `hl_open`；同号仓位绝对值变化不少于 20% 且变化名义达到币种门槛一半时生成 `hl_increase` 或 `hl_decrease`；非 0 到 0 生成 `hl_close`；符号改变 MUST 拆为一条按原仓位名义计算的 `hl_close` 与一条按新仓位名义计算的 `hl_open`。币种门槛 MUST 为 BTC/ETH 1,000,000 美元、配置的头部山寨 500,000 美元、其他币与 HIP-3 250,000 美元。

#### Scenario: 开仓刚好达到门槛
- **WHEN** BTC 仓位从 0 变为名义价值 1,000,000 美元的多仓
- **THEN** 系统生成一条 `hl_open`，方向为多，`amount_usd` 为 1,000,000 美元

#### Scenario: 加仓变化比例不足
- **WHEN** 已有仓位同向增加 19.99%，即使变化名义高于门槛一半
- **THEN** 系统更新仓位快照但不生成 `hl_increase`

#### Scenario: 加仓名义不足
- **WHEN** 头部山寨仓位同向增加 25%，但变化名义低于 250,000 美元
- **THEN** 系统更新仓位快照但不生成 `hl_increase`

#### Scenario: 反手拆分
- **WHEN** 钱包仓位从 8,000,000 美元多仓变为 6,000,000 美元空仓
- **THEN** 系统按同一检测时点生成 `hl_close` 与 `hl_open` 两个事件，分别保留原方向和新方向数据

### Requirement: 实时价格与接近强平检测
系统 SHALL 消费 `allMids` 作为本地实时标记价，并计算 `|标记价 − 强平价| / 标记价`。名义不少于 10,000,000 美元的仓位在距离不超过 5% 时为 A、不超过 2% 时为 S；其他仓位不超过 5% 时为 B。阈值状态 MUST 使用 1 个百分点滞回，避免价格在边界附近反复产生事件；同一强平风险簇从 A 进入 S MUST 触发一期升级补推。全仓强平价 MUST 在每次热点对账时刷新。

#### Scenario: 大仓进入五个百分点
- **WHEN** 12,000,000 美元仓位的强平距离从 6.1% 降至 5.0%
- **THEN** 系统生成 A 级 `hl_liq_risk`

#### Scenario: 风险从 A 升到 S
- **WHEN** 同一仓位在 30 分钟强平风险簇内由 4% 下降到 2%
- **THEN** 规则等级升为 S，并按一期推送决策安排 A→S 升级补推

#### Scenario: 滞回抑制边界抖动
- **WHEN** 已触发 5% 风险的仓位在 5.2% 与 4.9% 间反复波动但从未恢复到 6% 以上
- **THEN** 系统不重复生成新的 5% 阈值进入事件

### Requirement: 被清算识别
系统 SHALL 仅在成交 `liquidation.liquidatedUser` 与当前被监控钱包地址相等时生成 `hl_liquidated`。清算相关仓位名义不少于 5,000,000 美元时为 A，否则为 B。对手方成交即使携带 `liquidation` 对象也 MUST NOT 被误判为该钱包被清算。

#### Scenario: 被监控钱包确实被清算
- **WHEN** 成交的 `liquidation.liquidatedUser` 等于当前钱包且仓位名义为 5,000,000 美元
- **THEN** 系统生成 A 级 `hl_liquidated`

#### Scenario: 清算对手方不误报
- **WHEN** 当前钱包是清算成交对手方，而 `liquidation.liquidatedUser` 是另一个地址
- **THEN** 系统不为当前钱包生成 `hl_liquidated`

### Requirement: Hyperliquid 事件分级
系统 SHALL 通过注册到一期 `RuleEngine` 的 `hyperliquid` 规则分级。`hl_open`、`hl_increase`、`hl_decrease`、`hl_close` 按相关名义价值分级：不少于 50,000,000 美元为 S，不少于 5,000,000 美元为 A，其余已达到检测门槛的为 B；`hl_liq_risk`、`hl_liquidated` 按各自 Requirement 分级；`hl_new_whale` 为 B。设置杠杆不少于 20x 或实际杠杆不少于 10 MUST 作为事件属性展示而不单独生成事件。风格为做市/高频的钱包产生的所有 Hyperliquid 事件 MUST 降为 C。

#### Scenario: 开仓分级边界
- **WHEN** 普通方向型钱包开仓名义价值正好为 50,000,000 美元
- **THEN** `rule_level` 为 S；正好为 5,000,000 美元时为 A

#### Scenario: 高频钱包事件降级
- **WHEN** 风格为做市/高频的钱包发生 60,000,000 美元开仓
- **THEN** 事件仍入库但 `rule_level` 为 C，不产生推送

#### Scenario: 高杠杆仅作属性
- **WHEN** 钱包设置杠杆为 20x 但仓位没有达到任何事件检测条件
- **THEN** 系统不生成独立事件；下一次相关事件或页面展示包含高杠杆标记

### Requirement: 监控子源健康
系统 SHALL 分别以 `hl_rest` 与 `hl_core_ws` 登记重点 REST 监控和核心钱包 WS 健康状态。成功请求、有效状态消息或 pong MUST 更新对应 `last_success_at`；失败按一期阈值累计；无核心钱包时 `hl_core_ws` MUST 显示可区分的正常空闲模式而非异常。

#### Scenario: REST 与核心 WS 故障隔离
- **WHEN** 核心 WS 持续正常但重点 REST 连续失败 5 次
- **THEN** 状态页仅将 `hl_rest` 标为异常，`hl_core_ws` 保持正常

#### Scenario: 空核心名单不超时
- **WHEN** 核心名单为空且应用持续运行超过核心 WS 的正常 `stale_after`
- **THEN** `hl_core_ws` 以空闲/降级说明保持非异常，不发送错误健康告警

#### Scenario: 事件写入不登记内部来源
- **WHEN** Hyperliquid 监控事件通过一期 `EventSink` 入库
- **THEN** 系统不创建名为 `hl_monitor` 的 `collector_state` 行，状态页只展示已登记的健康子源

### Requirement: 监控运行时快速停止
系统 SHALL 在停止 Hyperliquid 运行时时立即取消受管后台任务及其在途 REST 请求，再关闭 WS 与 HTTP 资源，不得等待上游请求超时后才退出。

#### Scenario: 停止时存在未完成 REST 请求
- **WHEN** 运行时收到停止信号且一个 REST 请求仍未完成
- **THEN** 系统取消该请求并在 0.5 秒内完成运行时停止
