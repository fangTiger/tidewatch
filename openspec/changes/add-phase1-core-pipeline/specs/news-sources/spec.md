## ADDED Requirements

### Requirement: 采集器契约
系统 SHALL 提供轮询型 `PollCollector`（实现 `fetch(cursor) -> FetchResult`）与长连接型 `StreamCollector`（实现 `stream(cursor)` 与 `gap_fill(cursor)`）两种基类。每个采集器 MUST 声明 `name`、`group`、`proxy`（`global` 或 `direct`）、`stale_after`、`backfill_max`、`max_push_age`。基类 MUST 负责调度、限流、退避、把结果交给 `EventSink.ingest` 并在提交成功后才采用新游标。

#### Scenario: 提交成功后推进游标
- **WHEN** 轮询型采集器取回一批数据且 `EventSink.ingest` 成功返回
- **THEN** 下一次 `fetch` 使用新游标

#### Scenario: 提交失败保持旧游标
- **WHEN** `EventSink.ingest` 抛出异常
- **THEN** 采集器保持旧游标，并在退避后重试

### Requirement: 限流与退避
系统 SHALL 为每个采集器配置令牌桶限流；遇到 HTTP 429、5xx 或网络错误时 MUST 指数退避（1 秒起，每次 ×2，上限 5 分钟），成功一次后重置退避。

#### Scenario: 连续失败退避
- **WHEN** 采集器连续 3 次请求返回 503
- **THEN** 三次重试前的等待时间依次约为 1、2、4 秒

### Requirement: 健康判定
系统 SHALL 在每次请求成功（长连接收到任意消息或 pong）时更新 `collector_state.last_success_at`，产出事件时更新 `last_event_at`，失败时累加 `consecutive_failures`。采集器 MUST 在「连续失败 ≥ 5 次」或「`now − last_success_at > stale_after`」时判为异常；没有新数据 MUST NOT 视为失败。

#### Scenario: 长时间无新公告不告警
- **WHEN** OKX 公告接口持续请求成功但 3 小时没有新公告
- **THEN** `okx_ann` 状态保持正常

#### Scenario: 连续失败判为异常
- **WHEN** 金十接口连续 5 次请求失败
- **THEN** `jin10` 状态变为异常并触发健康告警

### Requirement: 断点补抓
系统 SHALL 在采集器启动时从 `collector_state.cursor` 继续采集，回溯时长 MUST NOT 超过该采集器的 `backfill_max`（一期三个采集器均为 6 小时）。

#### Scenario: 重启后补抓
- **WHEN** 进程停止 20 分钟后重启
- **THEN** 金十采集器从上次的快讯 ID 继续，补回这 20 分钟的快讯

### Requirement: 金十快讯采集
系统 SHALL 每 20 秒按增量 ID 轮询金十快讯（直连，不走代理），`occurred_at` 取快讯发布时间，`dedupe_key` 为 `jin10:{快讯ID}`，`max_push_age` 为 30 分钟，并保留源自带的 `important` 标记供分级使用。

#### Scenario: 解析快讯
- **WHEN** 金十接口返回一条 `important=1` 的快讯
- **THEN** 生成一条 `category = news`、`event_type = flash` 的事件，`raw` 中保留 `important` 字段

### Requirement: 币安公告采集
系统 SHALL 在配置了只读 API Key 与 Secret 时订阅币安公告 WS（`com_announcement_en`，每 30 秒 ping），并在每次（重）连接后以及每 10 分钟调用列表接口补缺；未配置时 MUST 退化为每 30 秒轮询列表接口，并在状态页标注「降级模式」。WS 与列表接口 MUST 使用同一去重键算法 `binance_ann:{sha1(规范化标题 + 发布日期)}`。

#### Scenario: WS 与列表接口拿到同一公告
- **WHEN** 同一条公告先经 WS 到达、随后又在补缺时从列表接口取回
- **THEN** 两者生成相同的 `dedupe_key`，只入库一次

#### Scenario: 未配置 API Key
- **WHEN** `.env` 中没有币安 API Key
- **THEN** `binance_ann` 以轮询模式运行，状态页显示「降级模式」

### Requirement: OKX 公告采集
系统 SHALL 每 30 秒轮询 OKX `/api/v5/support/announcements`，`occurred_at` 取 `pTime`，`dedupe_key` 为 `okx_ann:{公告 URL}`，`max_push_age` 为 30 分钟。

#### Scenario: 解析 OKX 公告
- **WHEN** 接口返回一条新的上币公告
- **THEN** 生成一条 `category = exchange` 的事件，`occurred_at` 等于公告的 `pTime`

### Requirement: 公告分类
系统 SHALL 把公告统一分类为 `spot_listing`、`perp_listing`、`delisting`、`perp_delisting`、`deposit_withdraw_suspend`、`other` 之一：币安按 `catalogId` 与英文标题关键词，OKX 按 `annType` 与标题关键词。

#### Scenario: 币安现货上币
- **WHEN** 币安公告标题为「Binance Will List XYZ (XYZ)」且属于新币上线类别
- **THEN** 事件的 `event_type` 为 `spot_listing`，`assets` 包含 XYZ

### Requirement: 新闻分级规则
系统 SHALL 按以下规则为金十快讯分级：`important=1` 且命中 S 关键词组（利率决议、FOMC、鲍威尔讲话、CPI、PCE、非农、紧急降息/加息、关税组合、ETF 获批、交易所被盗、稳定币脱锚、暂停提现、破产）为 S；命中 S 关键词组但非重要、或 `important=1` 且命中关注资产或 A 关键词组（美联储、央行、降息、加息、关税、制裁、SEC、证监会、ETF、战争、停火、黑客、被盗、破产）、或命中关注资产且同时出现特朗普/马斯克为 A；`important=1` 的其他快讯、或命中关注资产为 B；其他为 C。关键词组 MUST 可在配置中调整。

#### Scenario: 重要且命中 S 关键词
- **WHEN** 一条 `important=1` 的快讯标题为「美国 9 月非农就业人口 8.2 万，预期 12 万」
- **THEN** 事件 `rule_level` 为 S

#### Scenario: 重要但不涉及关注资产与关键词
- **WHEN** 一条 `important=1` 的快讯为「港股低开高走，恒指翻红」，不含关注资产与 A/S 关键词
- **THEN** 事件 `rule_level` 为 B

#### Scenario: 普通快讯
- **WHEN** 一条非重要快讯不含任何关注资产与关键词
- **THEN** 事件 `rule_level` 为 C

### Requirement: 交易所公告分级规则
系统 SHALL 按下表为公告分级：币安现货上币为 S、合约上线为 A；OKX 现货上币为 A、合约上线为 B；任一交易所下架关注资产为 A、下架其他资产为 B；合约下线为 B；暂停充提涉及关注资产为 A、其他为 C；其他公告为 C。

#### Scenario: OKX 下架关注资产
- **WHEN** OKX 发布下架 XRP 的公告，XRP 在关注资产中
- **THEN** 事件 `rule_level` 为 A

### Requirement: 关注资产识别
系统 SHALL 根据配置 `watchlist.assets`（每项含中英文别名）从标题与正文中识别资产，写入事件的 `assets`；识别不到时 `assets` 为空数组。

#### Scenario: 别名识别
- **WHEN** 快讯标题为「微策略再次增持比特币」，配置中 BTC 的别名包含「比特币」，MSTR 的别名包含「微策略」
- **THEN** 事件 `assets` 为 `{BTC, MSTR}`
