# 观潮（Tidewatch）

个人使用的市场监控系统：采集金十快讯与币安、OKX 交易所公告，按 S/A/B/C 分级，重要事件推送到飞书群，并在本机网站实时展示。完整设计见 `docs/plans/2026-09-30-tidewatch-design.md`。

## 环境

- macOS、Python 3.11、PostgreSQL 18（本机）
- 访问海外接口需要代理（在 `config.local.yaml` 设置 `proxy_url`）

## 安装

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env                                # 填写数据库连接、飞书 webhook 等
cp config.local.example.yaml config.local.yaml      # 填写代理、个人关注资产
alembic upgrade head
```

`.env` 与 `config.local.yaml` 已被 `.gitignore` 排除，不要提交。

## 运行

```bash
python -m tidewatch --dry-run        # 演练：不发送飞书，只写日志
python -m tidewatch                  # 正式运行（需要 FEISHU_WEBHOOK）
python -m tidewatch --only web       # 只启动网站
```

网站：http://127.0.0.1:8000（时间线）、http://127.0.0.1:8000/status（采集器状态与当日推送统计）

注意：`--only web` 单独运行时只提供已有数据的查询与展示，网站没有实时推送；实时更新只来自同一进程内运行的采集流水线。

## 开机自启（launchd）

```bash
deploy/install.sh            # 正式运行
deploy/install.sh --dry-run  # 演练模式常驻
deploy/uninstall.sh          # 卸载
```

注意：服务通过 `caffeinate -i` 阻止空闲睡眠，但**合上笔记本盖子仍会睡眠**，需要常开或外接显示器。

## 配置要点

- 阈值、关键词、频率：`config.yaml`（可在 `config.local.yaml` 按键覆盖；列表项按 `symbol`/`id` 合并，写 `remove: true` 删除默认项）
- 币安公告：`.env` 中配置只读的 `BINANCE_API_KEY` 与 `BINANCE_API_SECRET` 时使用 WebSocket 实时推送，否则每 30 秒轮询（状态页显示「降级」）
- 日志：`log/tidewatch_YYYY-MM-DD.log`，保留 14 天。`log/launchd.err.log` 只记录启动早期的错误且不会轮转，必要时请手动清理

## 飞书机器人与联调

在目标飞书群中依次进入「群设置 → 群机器人 → 添加机器人 → 自定义机器人」，创建后把 webhook 复制到 `.env` 的 `FEISHU_WEBHOOK`。在机器人的安全设置中勾选「签名校验」，把密钥复制到 `FEISHU_SECRET`；不要勾选「自定义关键词」或「IP 白名单」，否则观潮发送的卡片可能被拦截。

配置完成后可发送一组带「【联调测试】」标记的 S/A 事件卡片、A 级汇总与健康状态消息：

```bash
python -m tidewatch.tools.feishu_check
python -m tidewatch.tools.feishu_check --only s,a,digest,health
```

`--only` 可从 `s`、`a`、`digest`、`health` 中选择一个或多个分组（逗号分隔）。S 级测试会在群内 @所有人，请在合适的时间执行。

## 只读重新分级

用当前配置重新计算库中已有事件的 `rule_level`，比较新旧等级但绝不写回数据库。默认检查金十快讯；可限制最新事件数量，并用 `--changes` 显示等级变化明细（时间、旧→新、命中规则及标题前 60 字）。

```bash
python -m tidewatch.tools.regrade
python -m tidewatch.tools.regrade --source jin10 --limit 200 --changes
```

## 测试

```bash
pytest -q
```

测试库默认使用 `.env` 中数据库连接对应的 `tidewatch_test` 库。
