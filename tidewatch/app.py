"""组件装配、启动与优雅退出"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Awaitable

import uvicorn

from tidewatch.clock import Clock, utcnow
from tidewatch.collectors.base import Collector
from tidewatch.collectors.binance_ann import BinanceAnnPoll, BinanceAnnStream
from tidewatch.collectors.http import make_client
from tidewatch.collectors.jin10 import Jin10Collector
from tidewatch.collectors.okx_ann import OkxAnnCollector
from tidewatch.config import ConfigError, Settings, load_settings
from tidewatch.health import HealthMonitor
from tidewatch.hyperliquid.runtime import HyperliquidRuntime
from tidewatch.logging_setup import setup_logging
from tidewatch.notify.base import Notifier
from tidewatch.notify.dispatcher import Dispatcher
from tidewatch.notify.dryrun import DryRunNotifier
from tidewatch.notify.feishu import FeishuNotifier
from tidewatch.pipeline.sink import create_event_sink
from tidewatch.store.db import create_db_engine
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.embedded import EmbeddedServer
from tidewatch.web.live import LiveUpdates
from tidewatch.web.server import create_app

logger = logging.getLogger(__name__)

GROUPS = ("news", "hyperliquid", "web")
EVENT_GROUPS = frozenset({"news", "hyperliquid"})
REPO_ROOT = Path(__file__).resolve().parents[1]
STOP_TIMEOUT_S = 8
WEB_START_TIMEOUT_S = 10


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
        self.live = (
            LiveUpdates(self.engine, self.broadcaster, settings)
            if "web" in self.groups and bool(EVENT_GROUPS & self.groups)
            else None
        )
        self.collectors: list[Collector] = self._make_collectors() if "news" in self.groups else []
        self.hyperliquid = (
            HyperliquidRuntime(
                settings.hyperliquid,
                self.health,
                engine=self.engine,
                sink=self.sink,
                proxy_url=settings.proxy_url,
                clock=clock,
            )
            if "hyperliquid" in self.groups and settings.hyperliquid.enabled
            else None
        )
        if self.hyperliquid is not None:
            self.hyperliquid.register_health()
        self._tasks: list[asyncio.Task] = []
        self._collector_tasks: list[asyncio.Task] = []
        self._server: EmbeddedServer | None = None
        self._web_task: asyncio.Task | None = None  # uvicorn 的 serve 任务：单独保存，退出时等它自行结束而不是取消
        self._web_ready = False
        self._stopping = False

    # ---------- 装配 ----------

    def _make_notifier(self) -> Notifier:
        if self.dry_run or not (EVENT_GROUPS & self.groups):
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
        if EVENT_GROUPS & self.groups:
            if self.live is not None:
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
            self._tasks.append(asyncio.create_task(self.health.run(), name="health"))
            if self.live is not None:
                self._tasks.append(asyncio.create_task(self.live.run(), name="live"))
            self._collector_tasks = [
                asyncio.create_task(c.run(), name=f"collector-{c.name}") for c in self.collectors
            ]
            for task in self._collector_tasks:
                task.add_done_callback(_log_collector_exit)
            if self.hyperliquid is not None:
                await self.hyperliquid.start()
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
            self._server = EmbeddedServer(config)
            self._web_task = asyncio.create_task(self._server.serve(), name="web")
            self._web_task.add_done_callback(self._log_web_exit)
            try:
                await asyncio.wait_for(self._wait_web_started(), timeout=WEB_START_TIMEOUT_S)
            except asyncio.TimeoutError as exc:
                self._web_task.cancel()
                await asyncio.gather(self._web_task, return_exceptions=True)
                raise RuntimeError(
                    f"网站未完成启动：等待监听 {web.host}:{web.port} 超过 {WEB_START_TIMEOUT_S} 秒"
                ) from exc
            self._web_ready = True
            logger.info("网站已启动：http://%s:%s", web.host, web.port)
        mode = "，演练模式（不发送飞书）" if self.dry_run else ""
        logger.info("观潮已启动：%s%s", "、".join(sorted(self.groups)), mode)

    async def _wait_web_started(self) -> None:
        """等待 uvicorn 确认监听；任务提前结束时立即转交其异常"""
        assert self._server is not None and self._web_task is not None
        while True:
            if self._web_task.done():
                await self._web_task
                raise RuntimeError("网站启动失败：网站任务在启动前结束")
            if self._server.started:
                return
            await asyncio.sleep(0.01)

    def critical_tasks(self) -> tuple[asyncio.Task, ...]:
        """返回所有关键后台任务的只读快照。"""
        tasks = [
            *self._tasks,
            *self._collector_tasks,
            *self.sink.background_tasks(),
            *self.dispatcher.background_tasks(),
        ]
        if self.hyperliquid is not None:
            tasks.extend(self.hyperliquid.background_tasks())
        if self._web_task is not None:
            tasks.append(self._web_task)
        return tuple(dict.fromkeys(tasks))

    def _log_web_exit(self, task: asyncio.Task) -> None:
        """网站在完成启动后意外结束时记录错误"""
        if task.cancelled() or self._stopping or not self._web_ready:
            return
        exc = task.exception()
        if exc is not None:
            logger.error("网站任务异常退出：%s", exc, exc_info=exc)
        else:
            logger.error("网站任务意外结束")

    async def stop(self) -> None:
        """优雅退出：先停采集，再处理完已提交的写入与推送，最后关闭网站与各类连接

        对「部分启动」安全（组件未启动时对应步骤什么也不做）；各项资源关闭互不影响，
        任何一步出错只记录日志，数据库连接池一定会释放。
        """
        self._stopping = True
        try:
            for task in self._collector_tasks:
                task.cancel()
            await asyncio.gather(*self._collector_tasks, return_exceptions=True)
            if self.hyperliquid is not None:
                await self._guard("停止 Hyperliquid 子系统", self.hyperliquid.stop())
            await self._guard("停止推送调度器", self.dispatcher.stop())
            await self._guard("停止写入队列", self.sink.stop())
            # 先结束所有 SSE 订阅，让响应正常收尾；否则 sse-starlette 会在 uvicorn 退出时
            # 直接取消响应，uvicorn 随即记录 ERROR「ASGI callable returned without completing response」
            self.broadcaster.close()
            if self._server is not None:
                await self._wait_streams_closed()
                # 信号由应用接管，需主动通知 uvicorn 退出
                self._server.handle_exit(signal.SIGTERM, None)
            for task in self._tasks:
                task.cancel()
            waiting = [*self._tasks, *([self._web_task] if self._web_task is not None else [])]
            await asyncio.gather(*waiting, return_exceptions=True)
            for collector in self.collectors:
                if collector.client is not None:
                    await self._guard(f"关闭采集器 {collector.name} 的 HTTP 客户端", collector.client.aclose())
            await self._guard("关闭通知器", self.notifier.aclose())
        finally:
            await self.engine.dispose()
        logger.info("观潮已停止")

    @staticmethod
    async def _guard(what: str, awaitable: Awaitable[None]) -> None:
        """执行一个收尾步骤，出错只记录日志，不影响后续步骤"""
        try:
            await awaitable
        except Exception:
            logger.exception("%s失败", what)

    async def _wait_streams_closed(self, timeout: float = 1.0) -> None:
        """保险等待 SSE 响应收尾；Broadcaster.close() 已让流自然结束，这里只负责兜底等待"""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while self.broadcaster.subscriber_count > 0 and loop.time() < deadline:
            await asyncio.sleep(0.01)
        for _ in range(5):
            await asyncio.sleep(0)


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
    """解析命令行参数，返回所选组件组与演练模式设置"""
    parser = argparse.ArgumentParser(prog="tidewatch", description="观潮：个人市场监控系统")
    parser.add_argument("--only", nargs="+", choices=GROUPS, help="只启动指定组件组（默认全部）")
    parser.add_argument("--dry-run", action="store_true", help="演练模式：不发送飞书，只写日志")
    return parser.parse_args(argv)


async def run(settings: Settings, *, groups: set[str], dry_run: bool) -> int:
    """运行直到收到停止信号或关键任务意外结束；返回进程退出码"""
    app = Application(settings, groups=groups, dry_run=dry_run)
    stop_event = asyncio.Event()
    stop_waiter: asyncio.Task | None = None
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        loop.add_signal_handler(sig, stop_event.set)
    exit_code = 0
    try:
        try:
            await app.start()
        except Exception as exc:
            logger.exception("启动失败：%s", exc)
            exit_code = 1
        else:
            stop_waiter = asyncio.create_task(stop_event.wait(), name="stop-signal")
            critical = app.critical_tasks()
            done, _ = await asyncio.wait((stop_waiter, *critical), return_when=asyncio.FIRST_COMPLETED)
            if stop_waiter in done:
                logger.info("收到停止信号，正在退出…")
            else:
                exit_code = 1
                for task in critical:
                    if not task.done():
                        continue
                    if task.cancelled():
                        reason = "任务被取消"
                    else:
                        exc = task.exception()
                        reason = "正常返回" if exc is None else f"{type(exc).__name__}：{exc}"
                    logger.error(
                        "关键任务 %s 意外结束，进程将退出以便 launchd 重启：%s",
                        task.get_name(),
                        reason,
                    )
    finally:
        if stop_waiter is not None and not stop_waiter.done():
            stop_waiter.cancel()
            await asyncio.gather(stop_waiter, return_exceptions=True)
        # 启动失败时同样要清理已创建的资源（stop 对部分启动安全）
        try:
            await asyncio.wait_for(app.stop(), timeout=STOP_TIMEOUT_S)
        except asyncio.TimeoutError:
            logger.error("退出超时（%s 秒），强制结束", STOP_TIMEOUT_S)
        for sig in signals:
            loop.remove_signal_handler(sig)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    """加载配置并运行观潮，返回适合进程退出的状态码"""
    args = parse_args(argv)
    try:
        settings = load_settings(REPO_ROOT)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2
    setup_logging(settings.log, REPO_ROOT)
    try:
        return asyncio.run(run(settings, groups=set(args.only or GROUPS), dry_run=args.dry_run))
    except ConfigError as exc:
        logger.error("%s", exc)
        return 2
