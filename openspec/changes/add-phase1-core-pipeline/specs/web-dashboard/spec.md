## ADDED Requirements

### Requirement: 本机网站服务
系统 SHALL 在同一进程内提供网站服务，默认监听 `127.0.0.1:8000`，不需要登录；监听地址与端口 MUST 可在配置中修改。

#### Scenario: 访问首页
- **WHEN** 用户在本机浏览器打开 `http://127.0.0.1:8000/`
- **THEN** 返回首页时间线

### Requirement: 首页事件时间线
系统 SHALL 在首页按时间倒序展示事件簇，每个簇合并为一行，显示代表事件（已推送的簇为最近一次推送所用事件，未推送的簇为最高 `final_level` 中最早的一条）、时间（Asia/Shanghai）、等级、来源、资产与「来源 +N」。等级展示与筛选 MUST 使用 `final_level`。页面 MUST 支持按类别、等级、资产筛选，默认隐藏 C 级；过期未推送的补抓事件 MUST 标记「补抓」。

#### Scenario: 默认隐藏 C 级
- **WHEN** 用户打开首页且未选择任何筛选
- **THEN** 时间线不显示 C 级簇

#### Scenario: 按资产筛选
- **WHEN** 用户选择资产 BTC
- **THEN** 时间线只显示 `assets` 包含 BTC 的簇

### Requirement: 实时更新
系统 SHALL 通过 SSE 端点 `/stream` 推送 `event.created`、`cluster.updated`、`health.changed` 消息；首页 MUST 在收到消息后无需刷新即插入新簇或更新已有簇的来源数。消息 MUST 在对应事务提交之后发出。

#### Scenario: 新事件实时出现
- **WHEN** 首页已打开，此时一条新的 A 级快讯入库
- **THEN** 该事件在几秒内出现在时间线顶部，页面无需刷新

### Requirement: 状态页
系统 SHALL 在 `/status` 展示每个采集器的状态（正常/异常/降级）、最后成功时间、最后事件时间、最近错误与连续失败次数，以及当天推送统计（已送达、失败、过期、演练）。状态变化 MUST 通过 SSE 实时更新。

#### Scenario: 采集器异常标红
- **WHEN** 某采集器进入异常状态
- **THEN** 状态页该采集器行显示为红色并展示最近错误
