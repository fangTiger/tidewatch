## ADDED Requirements

### Requirement: 加权 REST 限流预算
系统 SHALL 为 Hyperliquid REST 请求按官方权重扣减令牌，默认使用彼此隔离的监控预算 600 权重/分钟与画像/排行榜预算 200 权重/分钟。令牌桶容量 MUST 默认为有效速率对应的 10 秒预算量且可配置，初始令牌 MUST 默认为 0，禁止用整分钟满桶制造启动突发。令牌桶 MUST 支持一次原子获取多个令牌，并保持一期采集器现有的满桶、单令牌调用默认行为。`clearinghouseState` 与 `spotClearinghouseState` 权重为 2；`userFills`/`userFillsByTime` 权重为 20 加每 20 条返回增加 1；`portfolio` 和 `userTwapSliceFills` 权重至少为 20，最终扣减 MUST 遵守上游实际返回量对应权重。

#### Scenario: 监控请求按权重等待
- **WHEN** 监控桶当前只有 1 个令牌且发起权重为 2 的 `clearinghouseState` 请求
- **THEN** 客户端等待补足第 2 个令牌后才发送请求，且不扣减画像/排行榜桶

#### Scenario: 批处理不能挤占监控预算
- **WHEN** 画像任务已耗尽 200 权重/分钟的批处理预算而监控桶仍有令牌
- **THEN** 新画像请求顺延，重点钱包监控请求仍可使用 600 权重/分钟预算发送

#### Scenario: 启动突发受短窗口约束
- **WHEN** Hyperliquid 子系统以默认配置启动并立即产生大量 REST 任务
- **THEN** 任意 10 秒内由本地初始令牌产生的额外突发不超过对应桶的 10 秒容量，且启动时不会预装整分钟预算

#### Scenario: 返回量增加成交查询权重
- **WHEN** `userFillsByTime` 返回超过 20 条成交
- **THEN** 本次请求除基础 20 权重外，按每 20 条增加 1 权重计入监控预算

### Requirement: REST 全局限流熔断与自适应速率
系统 SHALL 在任一 Hyperliquid REST 请求遇到 HTTP 429 或上游明确的限流响应时，暂停所有监控与批处理 REST 请求并进入共享冷却。冷却从 1 秒开始、连续限流时依次为 2、4 秒，最大 300 秒；冷却到期后最多放行一个探测请求，探测成功后重置冷却退避。被暂停的优先级对账任务 MUST 原样顺延且不得丢弃，单钱包失败退避 MUST NOT 对 429 另行快速重试。5xx 与网络错误继续使用请求级指数退避。一次冷却期只向 `hl_rest` 上报一次失败。

每次进入新的 429 冷却时，监控桶与批处理桶的有效速率 SHALL 分别乘以 50%，但不得低于可配置下限；连续 60 秒没有 429 后，每满 60 秒 SHALL 增加配置速率的 10%，直至恢复配置值。有效速率和相应短窗口容量变化 MUST 记录 INFO 日志。

#### Scenario: 连续限流响应
- **WHEN** 同一 REST 子源连续三次返回 HTTP 429
- **THEN** 全局冷却依次为约 1、2、4 秒，每次冷却到期只放行一个探测请求，监控与批处理请求均服从该冷却

#### Scenario: 成功后重置退避
- **WHEN** 一个请求在 8 秒退避后成功，随后下一次请求又返回 503
- **THEN** 新一轮退避从约 1 秒开始

#### Scenario: 429 后自适应恢复
- **WHEN** 当前有效速率收到一次 429 后连续 60 秒没有再次限流
- **THEN** 两个桶先各降至原有效速率的 50%（受配置下限保护），再按每分钟增加配置速率 10% 的步长恢复，且不超过配置值

#### Scenario: 冷却期间任务不丢失
- **WHEN** 优先级队列中的钱包对账因共享 429 冷却无法发送请求
- **THEN** 任务保留原地址、优先级和原因并顺延到冷却结束，不进入单钱包快速重试

### Requirement: Hyperliquid 代理选择
系统 SHALL 通过 `hyperliquid.proxy` 在 `global` 与 `direct` 间选择；默认值 MUST 为 `global`。选择 `global` 时 REST 与 WS MUST 共用一期 `proxy_url`，选择 `direct` 时二者 MUST 绕过代理。运维说明 MUST 明确限流按出口 IP 计算，切换代理节点会改变出口 IP。

#### Scenario: 默认使用全局代理
- **WHEN** `hyperliquid.proxy` 未被本地配置覆盖且 `proxy_url` 已设置
- **THEN** Hyperliquid REST 和 WS 都通过该全局代理建立连接

#### Scenario: 切换为直连
- **WHEN** `hyperliquid.proxy` 配置为 `direct`
- **THEN** Hyperliquid REST 和 WS 均不使用 `proxy_url`，其他采集器代理设置不受影响

### Requirement: WS 连接与订阅管理
系统 SHALL 集中管理 Hyperliquid WS 连接，每 50 秒发送 `{"method":"ping"}`，断线后指数退避重连并恢复订阅。每个出口 IP 的连接数 MUST NOT 超过 10，总订阅数 MUST NOT 超过 1,000，按用户订阅的用户数 MUST NOT 超过 10；超过上限的订阅请求 MUST 被拒绝并给出可诊断错误。

#### Scenario: 心跳保持连接
- **WHEN** WS 连接连续 50 秒没有需要发送的业务消息
- **THEN** 客户端发送一次协议 ping，并在收到 pong 或任意有效消息后更新子源成功时间

#### Scenario: 断线后恢复订阅
- **WHEN** 已订阅核心钱包与成交流的连接异常断开
- **THEN** 客户端退避重连并恢复断线前的有效订阅，不重复登记同一订阅

#### Scenario: 按用户订阅超过上限
- **WHEN** 已有 10 个核心钱包按用户订阅，又请求订阅第 11 个用户
- **THEN** 系统拒绝该订阅、保留现有订阅并输出中文配置错误

### Requirement: Hyperliquid 专用存储与迁移
系统 SHALL 在一期迁移之后的新 Alembic revision 中创建 `hl_wallets`、`hl_positions`、`hl_fills`、`hl_state`。`hl_wallets` MUST 保存地址主键、备注、层级、来源、排除状态、风格、`profile`、画像更新时间和最近有持仓时间；`hl_positions` MUST 以（地址, dex, 币种）唯一保存当前快照；`hl_fills` MUST 以（地址, `tid`）为主键幂等保存成交、盈亏、手续费、TWAP 与 liquidation 信息（同一成交的买卖双方共享 `tid`）；`hl_state` MUST 保存订阅、导入、批处理和对账游标。迁移 MUST 可降级回一期 head，且 MUST NOT 修改一期迁移文件。

#### Scenario: 从一期升级到二期
- **WHEN** 数据库当前 revision 为一期 `8f3bbaee6ef9` 并执行 `alembic upgrade head`
- **THEN** 四张 Hyperliquid 表及必要索引被创建，一期四张表与数据保持不变

#### Scenario: 二期迁移可逆
- **WHEN** 在二期 head 执行一次迁移降级
- **THEN** 仅删除二期四张表并回到一期 head，再次升级可重新创建相同结构

### Requirement: Hyperliquid 配置校验
系统 SHALL 提供 `hyperliquid` 配置段，至少包含启用状态、`proxy`、REST/WS 端点、600/200 权重预算、短窗口秒数、AIMD 最低预算、请求频率、排行榜周期、分层上限、事件门槛、头部山寨列表、行情条币种、核心钱包及五个子源的 `stale_after`。未知字段、非正预算、重复或非法地址、超过 10 个核心钱包 MUST 以中文错误拒绝启动。

#### Scenario: 核心钱包可为空
- **WHEN** `hyperliquid.core_wallets` 配置为空列表且其他配置合法
- **THEN** 配置校验通过，核心 WS 子源以无用户订阅状态启动

#### Scenario: 非法预算拒绝启动
- **WHEN** 监控预算配置为 0 或负数
- **THEN** 程序拒绝启动并在中文错误中指出 `hyperliquid` 对应预算项
