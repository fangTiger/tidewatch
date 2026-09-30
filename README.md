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
- 日志：`log/tidewatch_YYYY-MM-DD.log`，保留 14 天

## 测试

```bash
pytest -q
```

测试库默认使用 `.env` 中数据库连接对应的 `tidewatch_test` 库。
