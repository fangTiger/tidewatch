## ADDED Requirements

### Requirement: 巨鲸列表页
系统 SHALL 在 `/whales` 提供巨鲸列表，展示规范地址、备注/标签、层级、来源、账户规模、30 天盈亏、胜率、风格、是否有持仓和最近更新时间；MUST 支持按账户规模、30 天盈亏、胜率、风格、层级及是否有持仓筛选。页面 SHALL 提供添加地址、设置备注、标记核心和排除操作，并与 CLI 复用同一名单服务和上限/冲突校验。

#### Scenario: 组合筛选巨鲸
- **WHEN** 用户筛选「重点层、方向型、当前有持仓、30 天盈利」
- **THEN** 列表仅返回同时满足全部条件的钱包

#### Scenario: 页面添加地址
- **WHEN** 用户在 `/whales` 提交一个合法、未排除且不存在的地址
- **THEN** 地址以手动来源进入重点层，页面显示成功结果且刷新后仍存在

#### Scenario: 页面核心上限
- **WHEN** 已有 10 个核心钱包，用户从页面标记第 11 个核心
- **THEN** 请求被拒绝并显示中文原因，原名单不变

### Requirement: 钱包详情页
系统 SHALL 在 `/whales/{address}` 展示钱包 KPI、画像覆盖时间、盈亏曲线、当前各 dex 仓位、方向、数量、名义价值、开仓价、强平价、距强平进度、杠杆、未实现盈亏和最近操作。地址路由 MUST 规范化后查询；不存在的钱包 MUST 返回 404。缺失或不完整历史 MUST 明确标记，不得呈现为完整 30 天数据。

#### Scenario: 查看多 dex 钱包
- **WHEN** 钱包在主 dex 与 HIP-3 dex 都有仓位
- **THEN** 详情页分别展示各 dex 仓位，并用当前 `allMids` 计算可用的距强平进度

#### Scenario: 未知钱包返回 404
- **WHEN** 用户访问未在 `hl_wallets` 中的合法地址
- **THEN** 服务返回 404，而不是创建空钱包记录

### Requirement: 首页实时行情条
系统 SHALL 在首页顶部显示来自 Hyperliquid `allMids` 的实时行情，默认包括 BTC、ETH、SOL、HYPE，并允许通过配置调整币种与顺序。行情缺失或 `hl_trades_ws`/价格连接异常时 MUST 显示不可用或陈旧状态，不得继续把旧价显示为实时。

#### Scenario: 行情实时更新
- **WHEN** 首页已打开且 BTC 的 `allMids` 价格发生变化
- **THEN** 页面通过现有 `/stream` SSE 更新 BTC 价格，无需整页刷新

#### Scenario: 配置自定义行情币种
- **WHEN** 配置将行情币种设为 `[BTC, HYPE]`
- **THEN** 首页按该顺序只展示 BTC 与 HYPE

### Requirement: 首页巨鲸多空净仓位侧栏
系统 SHALL 在首页侧栏按币种聚合核心与重点钱包的多仓名义、空仓绝对名义和净仓位（多仓名义减空仓绝对名义），使用当前 `allMids` 估值。候选、已排除钱包和陈旧/无效快照 MUST NOT 计入；口径与数据更新时间 MUST 在侧栏可见。

#### Scenario: 净仓位计算
- **WHEN** 受监控钱包 BTC 多仓合计 30,000,000 美元、空仓绝对值合计 12,000,000 美元
- **THEN** 侧栏显示多 30,000,000、空 12,000,000、净多 18,000,000 美元

#### Scenario: 候选仓位不计入
- **WHEN** 候选钱包持有 50,000,000 美元 BTC 多仓但未进入重点或核心
- **THEN** 该仓位不影响首页巨鲸净仓位

### Requirement: Hyperliquid SSE 增量
系统 SHALL 复用一期 `/stream`，在事务提交或内存行情状态原子更新后广播 `wallet.updated`、`position.updated`、`market.updated` 与 `whales.net_position.updated`。浏览器 MUST 增量更新相关组件；队列溢出、SSE 重连或版本错序时 MUST 沿用 `resync`/全量刷新恢复一致状态。

#### Scenario: 仓位提交后广播
- **WHEN** 一个重点钱包仓位事务成功提交
- **THEN** 服务在提交后广播仓位与净仓位更新；提交回滚时不广播

#### Scenario: 慢客户端重同步
- **WHEN** 浏览器消费过慢导致 SSE 队列溢出
- **THEN** 服务发送 `resync`，浏览器全量刷新巨鲸数据、行情和侧栏

### Requirement: Hyperliquid 状态展示
系统 SHALL 在一期 `/status` 页分别展示 `hl_rest`、`hl_trades_ws`、`hl_core_ws`、`hl_leaderboard`、`hl_profile` 的状态、模式、最后成功、最后事件、最近错误与连续失败次数。低频任务的状态 MUST 使用各自周期配置的 `stale_after`，不得按实时源阈值判定。

#### Scenario: 排行榜与画像独立显示
- **WHEN** 排行榜导入异常但昨晚画像批处理成功
- **THEN** 状态页将 `hl_leaderboard` 标红，同时 `hl_profile` 保持正常并显示自己的最后成功时间
