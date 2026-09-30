## ADDED Requirements

### Requirement: 配置分层与校验
系统 SHALL 依次加载 `config.yaml`（公开默认值）、`config.local.yaml`（私有覆盖，深度合并）与 `.env`（密钥），并用 pydantic 校验。校验失败时 MUST 以中文错误信息指出出错的配置项并拒绝启动。`config.local.yaml` 与 `.env` MUST 被 `.gitignore` 排除，仓库中 MUST 提供 `config.local.example.yaml` 与 `.env.example`。

#### Scenario: 本地配置覆盖默认值
- **WHEN** `config.yaml` 中 `watchlist.assets` 为默认列表，`config.local.yaml` 中新增一个资产
- **THEN** 运行时的关注资产包含默认列表与新增资产

#### Scenario: 配置错误
- **WHEN** `config.yaml` 中某采集器的 `interval` 为负数
- **THEN** 程序拒绝启动并输出指明该配置项的中文错误

### Requirement: 命令行参数
系统 SHALL 以 `python -m tidewatch` 启动，支持 `--only <组…>`（一期可选组为 `news`、`web`，不带参数时全部启动）与 `--dry-run`（使用演练通知器，不发送飞书）。流水线与推送 MUST 随任何产生事件的组自动启动。

#### Scenario: 只启动网站
- **WHEN** 以 `--only web` 启动
- **THEN** 只运行网站服务，不启动任何采集器

### Requirement: 日志
系统 SHALL 将日志写入 `log/tidewatch_YYYY-MM-DD.log`，按天轮转、保留 14 天，日志消息使用中文，并同时输出到标准输出。

#### Scenario: 日志文件
- **WHEN** 程序运行时
- **THEN** 当天日志写入 `log/tidewatch_<当天日期>.log`

### Requirement: 数据库迁移
系统 SHALL 使用 Alembic 管理表结构，一期迁移创建 `events`、`event_clusters`、`collector_state`、`push_log` 四张表及必要索引（`dedupe_key` 唯一索引、按 `collected_at` 与类别查询的索引）。数据库连接串 MUST 从 `.env` 读取。

#### Scenario: 初始化数据库
- **WHEN** 在空的 `tidewatch` 库执行 `alembic upgrade head`
- **THEN** 四张表及索引创建成功，再执行 `alembic downgrade base` 可全部删除

### Requirement: 代理设置
系统 SHALL 从配置读取全局代理地址，每个采集器按其 `proxy` 设置选择走代理或直连。

#### Scenario: 金十直连
- **WHEN** 全局代理已配置，`jin10` 的 `proxy` 为 `direct`
- **THEN** 金十请求不经过代理，币安与 OKX 请求经过代理

### Requirement: 守护与自启
系统 SHALL 提供 launchd 配置 `deploy/com.captain.tidewatch.plist`（`RunAtLoad`、`KeepAlive`，通过 `caffeinate -i` 启动）以及安装、卸载脚本；进程异常退出后 MUST 由 launchd 自动拉起。

#### Scenario: 崩溃后自动重启
- **WHEN** 已安装 launchd 服务且进程被强制结束
- **THEN** launchd 在数秒内重新启动进程

### Requirement: 优雅退出
系统 SHALL 在收到 SIGTERM 或 SIGINT 时停止接收新数据、等待 `EventSink` 处理完当前命令、关闭网络连接与数据库连接池后退出。

#### Scenario: 收到停止信号
- **WHEN** 进程收到 SIGTERM
- **THEN** 进程在 10 秒内退出，且没有写了一半的事务
