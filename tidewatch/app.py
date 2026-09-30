"""组件装配、启动与优雅退出"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from pathlib import Path

import uvicorn

from tidewatch.clock import Clock, utcnow
from tidewatch.collectors.base import Collector
from tidewatch.collectors.binance_ann import BinanceAnnPoll, BinanceAnnStream
from tidewatch.collectors.http import make_client
from tidewatch.collectors.jin10 import Jin10Collector
from tidewatch.collectors.okx_ann import OkxAnnCollector
from tidewatch.config import ConfigError, Settings, load_settings
from tidewatch.health import HealthMonitor
from tidewatch.logging_setup import setup_logging
from tidewatch.notify.base import Notifier
from tidewatch.notify.dispatcher import Dispatcher
from tidewatch.notify.dryrun import DryRunNotifier
from tidewatch.notify.feishu import FeishuNotifier
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.db import create_db_engine
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.live import LiveUpdates
from tidewatch.web.server import create_app

logger = logging.getLogger(__name__)

GROUPS = ("news", "web")
REPO_ROOT = Path(__file__).resolve().parents[1]
STOP_TIMEOUT_S = 8


class _EmbeddedServer(uvicorn.Server):
    """嵌入同一事件循环的 uvicorn：不接管信号，由应用统一处理"""

    @contextlib.contextmanager
    def capture_signals(self):
        yield

    def install_signal_handlers(self) -> None:  # 兼容旧版 uvicorn
        pass


class Application:
    def __init__(self, settings: Settings, *, groups: set[str], dry_run: bool, clock: Clock = utcnow) -> None:
        self.settings = settings
        self.groups = set(groups)
        self.dry_run = dry_run
        self.clock = clock
        self.notifier = self._make_notifier()
        self.engine = create_db_engine(settings.secrets.database_url)
        self.broadcaster = Broadcaster()
        self.health = HealthMonitor(self.engine, self.notifier, settings.dispatch, clock=clock)
        self.sink = create_event_sink(self.engine, settings, clock=clock)
        self.dispatcher = Dispatcher(self.engine, self.sink, self.notifier, settings.dispatch, clock=clock)
        self.live = LiveUpdates(self.engine, self.broadcaster, settings)
        self.collectors: list[Collector] = self._make_collectors() if "news" in self.groups else []
        self._tasks: list[asyncio.Task] = []
        self._collector_tasks: list[asyncio.Task] = []
        self._server: _EmbeddedServer | None = None

    # ---------- 装配 ----------

    def _make_notifier(self) -> Notifier:
        if self.dry_run or "news" not in self.groups:
            return DryRunNotifier()
        webhook = self.settings.secrets.feishu_webhook
        if not webhook:
            raise ConfigError(
                "未配置飞书 webhook：请在 .env 中设置 FEISHU_WEBHOOK（建议同时设置 FEISHU_SECRET），或使用 --dry-run 演练"
            )
        return FeishuNotifier(webhook, self.settings.secrets.feishu_secret)

    def _make_collectors(self) -> list[Collector]:
        settings, secrets = self.settings, self.settings.secrets
        common = {"sink": self.sink, "health": self.health, "engine": self.engine, "clock": self.clock}

        def client_for(name: str):
            return make_client(settings.collector(name).proxy, settings.proxy_url)

        collectors: list[Collector] = []
        if settings.collector("jin10").enabled:
            collectors.append(Jin10Collector(settings.collector("jin10"), client=client_for("jin10"), **common))
        if settings.collector("okx_ann").enabled:
            collectors.append(OkxAnnCollector(settings.collector("okx_ann"), client=client_for("okx_ann"), **common))
        binance_cfg = settings.collector("binance_ann")
        if binance_cfg.enabled:
            if secrets.binance_api_key and secrets.binance_api_secret:
                collectors.append(
                    BinanceAnnStream(
                        binance_cfg,
                        api_key=secrets.binance_api_key,
                        api_secret=secrets.binance_api_secret,
                        ws_proxy=settings.proxy_url if binance_cfg.proxy == "global" else None,
                        client=client_for("binance_ann"),
                        **common,
                    )
                )
            else:
                collectors.append(BinanceAnnPoll(binance_cfg, client=client_for("binance_ann"), **common))
        for collector in collectors:
            self.health.register(collector.name, collector.cfg.stale_after_s)
        return collectors

    @property
    def binance_degraded(self) -> bool:
        return any(isinstance(c, BinanceAnnPoll) for c in self.collectors)

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        if "news" in self.groups:
            self.sink.on_ingest(self.live.on_ingest)
            self.health.on_change(self.live.on_health)
            self.dispatcher.attach()
            await self.health.load()
            await self.sink.start()
            await self.dispatcher.start()
            if any(c.name == "binance_ann" for c in self.collectors):
                # 每次启动都按实际情况设置，补上 API Key 后状态页会恢复「正常」
                await self.health.set_mode("binance_ann", "degraded" if self.binance_degraded else "normal")
                if self.binance_degraded:
                    logger.warning("未配置币安只读 API Key，币安公告以降级模式（轮询列表接口）运行")
            self._tasks += [
                asyncio.create_task(self.health.run(), name="health"),
                asyncio.create_task(self.live.run(), name="live"),
            ]
            self._collector_tasks = [
                asyncio.create_task(c.run(), name=f"collector-{c.name}") for c in self.collectors
            ]
            for task in self._collector_tasks:
                task.add_done_callback(_log_collector_exit)
        if "web" in self.groups:
            web = self.settings.web
            config = uvicorn.Config(
                create_app(self.engine, self.broadcaster, self.settings, clock=self.clock),
                host=web.host,
                port=web.port,
                log_config=None,
                access_log=False,
                timeout_graceful_shutdown=3,  # 浏览器保持 SSE 连接时也能按时退出
            )
            self._server = _EmbeddedServer(config)
            self._tasks.append(asyncio.create_task(self._server.serve(), name="web"))
            logger.info("网站已启动：http://%s:%s", web.host, web.port)
        mode = "，演练模式（不发送飞书）" if self.dry_run else ""
        logger.info("观潮已启动：%s%s", "、".join(sorted(self.groups)), mode)

    async def stop(self) -> None:
        """优雅退出：先停采集，再处理完已提交的写入与推送，最后关闭网站与各类连接"""
        for task in self._collector_tasks:
            task.cancel()
        await asyncio.gather(*self._collector_tasks, return_exceptions=True)
        await self.dispatcher.stop()
        await self.sink.stop()
        if self._server is not None:
            # 信号由应用接管，需主动通知 uvicorn 退出（会一并关闭 SSE 连接）
            self._server.handle_exit(signal.SIGTERM, None)
        for task in self._tasks:
            if task.get_name() != "web":
                task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for collector in self.collectors:
            if collector.client is not None:
                await collector.client.aclose()
        await self.notifier.aclose()
        await self.engine.dispose()
        logger.info("观潮已停止")


def _log_collector_exit(task: asyncio.Task) -> None:
    """采集器任务意外结束时记录错误（正常情况下只会被取消）"""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("采集器任务 %s 异常退出：%s", task.get_name(), exc, exc_info=exc)
    else:
        logger.error("采集器任务 %s 意外结束", task.get_name())


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="tidewatch", description="观潮：个人市场监控系统")
    parser.add_argument("--only", nargs="+", choices=GROUPS, help="只启动指定组件组（默认全部）")
    parser.add_argument("--dry-run", action="store_true", help="演练模式：不发送飞书，只写日志")
    return parser.parse_args(argv)


async def run(settings: Settings, *, groups: set[str], dry_run: bool) -> None:
    app = Application(settings, groups=groups, dry_run=dry_run)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await app.start()
    try:
        await stop_event.wait()
    finally:
        logger.info("收到停止信号，正在退出…")
        try:
            await asyncio.wait_for(app.stop(), timeout=STOP_TIMEOUT_S)
        except asyncio.TimeoutError:
            logger.error("退出超时（%s 秒），强制结束", STOP_TIMEOUT_S)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        settings = load_settings(REPO_ROOT)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2
    setup_logging(settings.log, REPO_ROOT)
    try:
        asyncio.run(run(settings, groups=set(args.only or GROUPS), dry_run=args.dry_run))
    except ConfigError as exc:
        logger.error("%s", exc)
        return 2
    return 0
