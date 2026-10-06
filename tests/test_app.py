import asyncio
import gc
import logging
import os
import signal
import time
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select

import tidewatch.app as app_module
from fakes import free_port, wait_until
from tidewatch.app import GROUPS, Application, parse_args, run
from tidewatch.collectors.binance_ann import BinanceAnnPoll, BinanceAnnStream
from tidewatch.collectors.jin10 import Jin10Collector
from tidewatch.collectors.okx_ann import OkxAnnCollector
from tidewatch.config import ConfigError
from tidewatch.notify.dryrun import DryRunNotifier
from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.rules.engine import build_rule_engine
from tidewatch.store.repo import load_cursor
from tidewatch.store.tables import collector_state, event_clusters, events, hl_wallets
from tidewatch.web.embedded import EmbeddedServer


def test_parse_args():
    assert parse_args([]).only is None and parse_args([]).dry_run is False
    args = parse_args(["--only", "web", "--dry-run"])
    assert args.only == ["web"] and args.dry_run is True
    with pytest.raises(SystemExit):
        parse_args(["--only", "unknown"])


def test_parse_args_accepts_hyperliquid_group():
    assert "hyperliquid" in GROUPS
    args = parse_args(["--only", "hyperliquid", "web", "--dry-run"])
    assert args.only == ["hyperliquid", "web"]


async def test_requires_feishu_unless_dry_run(settings):
    with pytest.raises(ConfigError) as exc:
        Application(settings, groups={"news"}, dry_run=False)
    assert "FEISHU_WEBHOOK" in str(exc.value)
    web_only = Application(settings, groups={"web"}, dry_run=False)  # 只开网站不需要飞书
    assert isinstance(web_only.notifier, DryRunNotifier) and web_only.collectors == []
    await web_only.stop()


async def test_hyperliquid_is_an_event_group_for_notifier(settings):
    """Hyperliquid 单组正式运行时不能因没有 news 而退化为演练通知器。"""
    with pytest.raises(ConfigError) as exc:
        Application(settings, groups={"hyperliquid"}, dry_run=False)
    assert "FEISHU_WEBHOOK" in str(exc.value)

    settings.secrets.feishu_webhook = "https://example.com/hook"
    app = Application(settings, groups={"hyperliquid"}, dry_run=False)
    try:
        assert app.notifier.__class__.__name__ == "FeishuNotifier"
        assert app.collectors == []
    finally:
        await app.stop()


async def test_collectors_and_binance_mode(settings):
    app = Application(settings, groups={"news"}, dry_run=True)
    assert [type(c) for c in app.collectors] == [Jin10Collector, OkxAnnCollector, BinanceAnnPoll]
    assert app.binance_degraded is True
    await app.stop()
    settings.secrets.binance_api_key, settings.secrets.binance_api_secret = "k", "s"
    app = Application(settings, groups={"news"}, dry_run=True)
    assert isinstance(app.collectors[-1], BinanceAnnStream) and app.binance_degraded is False
    await app.stop()


async def test_start_and_stop_cleanly(engine, settings):
    app = Application(settings, groups={"news"}, dry_run=True)
    for collector in app.collectors:  # 测试中不访问外部接口
        await collector.client.aclose()
    app.collectors = []
    await app.start()
    await app.stop()


# ---------- 启动失败与部分启动 ----------


async def _start_web(settings) -> tuple[Application, str]:
    """在空闲端口上启动只含网站的应用，等到 uvicorn 完成启动后返回应用与基础地址"""
    settings.web.port = free_port()
    app = Application(settings, groups={"web"}, dry_run=True)
    await app.start()
    await wait_until(lambda: app._server.started)  # 启动失败时超时报错，不会挂起
    return app, f"http://127.0.0.1:{settings.web.port}"


async def test_run_returns_nonzero_when_start_fails(settings, caplog):
    """数据库不可达时 run() 记录「启动失败」、清理资源并返回非零，而不是抛出异常"""
    settings.secrets.database_url = "postgresql+asyncpg://x:y@127.0.0.1:1/none"
    with caplog.at_level(logging.INFO):
        code = await asyncio.wait_for(run(settings, groups={"news"}, dry_run=True), timeout=20)
    assert code != 0
    failed = [r for r in caplog.records if "启动失败" in r.getMessage()]
    assert failed and failed[0].exc_info is not None
    assert any("观潮已停止" in r.getMessage() for r in caplog.records)  # stop() 已执行
    gc.collect()  # 有未关闭的连接或未等待的协程时，-W error 会在此处暴露


async def test_stop_after_partial_start(engine, settings):
    """启动到一半失败（调度器启动抛异常）后，stop() 不抛异常并释放已启动的组件"""
    app = Application(settings, groups={"news"}, dry_run=True)

    async def broken_start() -> None:
        raise RuntimeError("调度器启动失败")

    app.dispatcher.start = broken_start
    with pytest.raises(RuntimeError, match="调度器启动失败"):
        await app.start()
    await asyncio.wait_for(app.stop(), timeout=5)
    assert app.sink._worker is None


async def test_stop_closes_everything_even_if_close_fails(settings, caplog):
    """采集器 client、notifier 的关闭各自出错时只记录日志，engine.dispose() 一定执行"""
    app = Application(settings, groups={"web"}, dry_run=True)
    for collector in app.collectors:
        await collector.client.aclose()
    calls: list[str] = []

    class BrokenClient:
        async def aclose(self) -> None:
            calls.append("client")
            raise RuntimeError("client 关闭失败")

    async def broken_notifier_close() -> None:
        calls.append("notifier")
        raise RuntimeError("notifier 关闭失败")

    class TrackedEngine:
        """AsyncEngine.dispose 是只读属性，用包装对象记录调用"""

        def __init__(self, real) -> None:
            self.real = real

        async def dispose(self) -> None:
            calls.append("dispose")
            await self.real.dispose()

    real_engine = app.engine
    app.collectors = [
        SimpleNamespace(name="a", client=BrokenClient()),
        SimpleNamespace(name="b", client=BrokenClient()),
    ]
    app.notifier.aclose = broken_notifier_close
    app.engine = TrackedEngine(real_engine)
    with caplog.at_level(logging.ERROR):
        await asyncio.wait_for(app.stop(), timeout=5)
    assert calls == ["client", "client", "notifier", "dispose"]  # 前面的失败不影响后面的关闭
    messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert sum("关闭" in m and "失败" in m for m in messages) == 3  # 两个采集器 client + 通知器各记录一条


def test_main_returns_run_exit_code(monkeypatch, settings):
    monkeypatch.setattr(app_module, "load_settings", lambda root: settings)
    monkeypatch.setattr(app_module, "setup_logging", lambda cfg, root: None)
    for code in (0, 1):
        async def fake_run(settings, *, groups, dry_run, _code=code) -> int:
            return _code

        monkeypatch.setattr(app_module, "run", fake_run)
        assert app_module.main(["--dry-run"]) == code


# ---------- 网站（真实嵌入的 uvicorn） ----------


async def test_only_web_serves_index_then_stops(engine, settings):
    app, base = await _start_web(settings)
    try:
        async with httpx.AsyncClient(base_url=base, timeout=5) as http:
            response = await http.get("/")
        assert response.status_code == 200 and "观潮" in response.text
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)
    assert app._server.should_exit


async def test_web_task_saved_separately(engine, settings):
    """uvicorn 的任务单独保存，不混在后台任务列表里；stop() 会等它结束"""
    app, _ = await _start_web(settings)
    try:
        assert isinstance(app._web_task, asyncio.Task) and not app._web_task.done()
        assert app._web_task not in app._tasks
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)
    assert app._web_task.done()


async def test_stop_ends_sse_streams_cleanly(engine, settings, caplog):
    """退出时 SSE 连接正常结束：stop() 很快完成，uvicorn 不打印「ASGI callable returned without completing response」"""
    app, base = await _start_web(settings)
    try:
        async with httpx.AsyncClient(base_url=base, timeout=5) as http:
            async with http.stream("GET", "/stream") as response:
                assert response.status_code == 200
                await wait_until(lambda: app.broadcaster.subscriber_count == 1)
                with caplog.at_level(logging.INFO):
                    started = time.monotonic()
                    await asyncio.wait_for(app.stop(), timeout=3)
                    elapsed = time.monotonic() - started
                # 响应被正常完成：客户端读到完整的（空）响应体，而不是连接被突然切断
                await asyncio.wait_for(response.aread(), timeout=3)
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)
    assert elapsed < 2, f"stop() 耗时 {elapsed:.1f} 秒，应不必等待 uvicorn 的优雅退出超时"
    errors = [r.getMessage() for r in caplog.records if r.name == "uvicorn.error" and r.levelno >= logging.ERROR]
    assert errors == []


async def test_run_exits_zero_on_sigterm(engine, settings, caplog):
    """收到 SIGTERM 后 run() 正常退出并返回 0（只有信号处理器装好后才发送信号，避免杀掉测试进程）"""
    settings.web.port = free_port()
    task = asyncio.create_task(run(settings, groups={"web"}, dry_run=True))
    try:
        await wait_until(lambda: signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, signal.SIG_IGN, None))
        await asyncio.sleep(0.3)  # 让应用完成启动
        with caplog.at_level(logging.INFO, logger="tidewatch.app"):
            os.kill(os.getpid(), signal.SIGTERM)
            assert await asyncio.wait_for(task, timeout=10) == 0
        assert not any("关键任务" in record.getMessage() for record in caplog.records)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_run_sigterm_waits_for_inflight_batch_atomically(engine, settings, monkeypatch):
    """规范场景：SIGTERM 等待已提交批次完成，事件、簇与游标不会留下半截状态。"""

    class BlockingRules:
        def __init__(self) -> None:
            self.real = build_rule_engine(settings)
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def evaluate(self, event, ctx):
            self.entered.set()
            await self.release.wait()
            return await self.real.evaluate(event, ctx)

    blocking = BlockingRules()
    real_create_sink = app_module.create_event_sink

    def create_blocking_sink(db_engine, app_settings, *, clock):
        return real_create_sink(db_engine, app_settings, clock=clock, rules=blocking)

    now = app_module.utcnow()
    batch = FetchResult(
        [
            Event("jin10", "news", "flash", now - timedelta(minutes=2), "美国公布非农数据", "shutdown:1", raw={"important": 1}),
            Event("jin10", "news", "flash", now - timedelta(minutes=1), "比特币突破新高", "shutdown:2", raw={"important": 0}),
        ],
        {"last_id": "shutdown:2"},
    )

    class BatchCollector:
        name = "shutdown_test"
        cfg = SimpleNamespace(stale_after_s=300)
        client = None

        def __init__(self, sink) -> None:
            self.sink = sink

        async def run(self) -> None:
            await self.sink.ingest(batch, collector=self.name)
            await asyncio.Event().wait()

    def make_collectors(app: Application):
        app.health.register(BatchCollector.name, BatchCollector.cfg.stale_after_s)
        return [BatchCollector(app.sink)]

    monkeypatch.setattr(app_module, "create_event_sink", create_blocking_sink)
    monkeypatch.setattr(Application, "_make_collectors", make_collectors)
    task = asyncio.create_task(run(settings, groups={"news"}, dry_run=True))
    try:
        await asyncio.wait_for(blocking.entered.wait(), timeout=5)
        await wait_until(lambda: signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, signal.SIG_IGN, None))
        os.kill(os.getpid(), signal.SIGTERM)
        blocking.release.set()
        assert await asyncio.wait_for(task, timeout=10) == 0
    finally:
        blocking.release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async with engine.connect() as conn:
        event_rows = (await conn.execute(select(events.c.dedupe_key, events.c.cluster_id))).all()
        cluster_ids = set((await conn.execute(select(event_clusters.c.id))).scalars())
        cursor = await load_cursor(conn, BatchCollector.name)

    if event_rows:
        assert {row.dedupe_key for row in event_rows} == {"shutdown:1", "shutdown:2"}
        assert all(row.cluster_id in cluster_ids for row in event_rows)
        assert cursor == {"last_id": "shutdown:2"}
    else:
        assert cluster_ids == set()
        assert cursor is None


# ---------- 关键任务守护 ----------


class _CrashingCollector:
    """启动后立即崩溃的采集器替身。"""

    name = "crashing"
    cfg = SimpleNamespace(stale_after_s=300)
    client = None

    async def run(self) -> None:
        raise RuntimeError("采集器崩溃")


def _make_crashing_collector(app: Application):
    app.health.register(_CrashingCollector.name, _CrashingCollector.cfg.stale_after_s)
    return [_CrashingCollector()]


async def test_run_returns_nonzero_when_collector_task_crashes(engine, settings, monkeypatch, caplog):
    """采集器关键任务抛异常时，run() 主动收尾并返回非零。"""
    monkeypatch.setattr(Application, "_make_collectors", _make_crashing_collector)
    with caplog.at_level(logging.INFO, logger="tidewatch.app"):
        code = await asyncio.wait_for(run(settings, groups={"news"}, dry_run=True), timeout=5)
    assert code == 1
    assert any(
        "关键任务 collector-crashing 意外结束，进程将退出以便 launchd 重启" in record.getMessage()
        for record in caplog.records
    )
    assert any("观潮已停止" in record.getMessage() for record in caplog.records)


async def test_run_returns_nonzero_when_health_task_returns(engine, settings, monkeypatch, caplog):
    """健康监控关键任务正常返回也属于意外结束。"""
    monkeypatch.setattr(Application, "_make_collectors", lambda app: [])

    async def return_immediately(self) -> None:
        return None

    monkeypatch.setattr(app_module.HealthMonitor, "run", return_immediately)
    with caplog.at_level(logging.INFO, logger="tidewatch.app"):
        code = await asyncio.wait_for(run(settings, groups={"news"}, dry_run=True), timeout=5)
    assert code == 1
    assert any(
        "关键任务 health 意外结束，进程将退出以便 launchd 重启：正常返回" in record.getMessage()
        for record in caplog.records
    )


async def test_application_exposes_all_component_background_tasks(engine, settings):
    """应用守护范围包含 sink、调度器和应用直接创建的全部后台任务。"""
    app = Application(settings, groups={"news"}, dry_run=True)
    await _quiet(app)
    await asyncio.wait_for(app.start(), timeout=5)
    try:
        names = {task.get_name() for task in app.critical_tasks()}
        assert names >= {
            "health",
            "event-sink",
            "dispatch-route",
            "dispatch-send",
            "dispatch-send-s",
            "dispatch-digest",
            "dispatch-retry",
        }
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)


# ---------- 网站启动失败：端口被占用 ----------


async def test_run_returns_1_when_port_in_use(engine, settings, caplog, occupied_port):
    """端口被占用：run() 记录原因、清理资源并返回 1；SystemExit 不会穿出事件循环"""
    settings.web.port = occupied_port
    with caplog.at_level(logging.INFO):
        code = await asyncio.wait_for(run(settings, groups={"web"}, dry_run=True), timeout=20)
    assert code == 1
    messages = [r.getMessage() for r in caplog.records]
    assert any("端口可能被占用" in m for m in messages)
    failed = [r for r in caplog.records if "启动失败" in r.getMessage() and r.exc_info is not None]
    assert failed  # 走的是 run() 的「启动失败」路径
    assert not any("网站已启动" in m or "观潮已启动" in m for m in messages)
    assert any("观潮已停止" in m for m in messages)  # 已清理资源
    gc.collect()


async def test_start_raises_when_port_in_use(engine, settings, caplog, occupied_port):
    """start() 在网站没有真正监听成功时抛 RuntimeError，不输出「已启动」日志"""
    settings.web.port = occupied_port
    app = Application(settings, groups={"web"}, dry_run=True)
    try:
        with caplog.at_level(logging.INFO):
            with pytest.raises(RuntimeError, match="端口可能被占用"):
                await asyncio.wait_for(app.start(), timeout=20)
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)
    assert not any("已启动" in r.getMessage() for r in caplog.records)


async def test_start_returns_only_after_web_listening(engine, settings, caplog):
    """start() 返回时 uvicorn 已经开始监听；「网站已启动」日志在监听成功之后才输出"""
    settings.web.port = free_port()
    app = Application(settings, groups={"web"}, dry_run=True)
    started_when_logged: list[bool] = []

    class Spy(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if "网站已启动" in record.getMessage():
                started_when_logged.append(app._server.started)

    spy = Spy()
    logging.getLogger("tidewatch.app").addHandler(spy)
    try:
        with caplog.at_level(logging.INFO):
            await asyncio.wait_for(app.start(), timeout=20)
        assert app._server.started
        assert started_when_logged == [True]
    finally:
        logging.getLogger("tidewatch.app").removeHandler(spy)
        await asyncio.wait_for(app.stop(), timeout=5)


async def test_start_times_out_when_web_never_starts(engine, settings, monkeypatch):
    """网站迟迟没有进入「已启动」：超时后抛 RuntimeError 并取消网站任务，不会挂起"""
    settings.web.port = free_port()

    async def hang(self, sockets=None):
        await asyncio.sleep(3600)

    monkeypatch.setattr(EmbeddedServer, "serve", hang)
    monkeypatch.setattr(app_module, "WEB_START_TIMEOUT_S", 0.3)
    app = Application(settings, groups={"web"}, dry_run=True)
    try:
        with pytest.raises(RuntimeError, match="未完成启动"):
            await asyncio.wait_for(app.start(), timeout=10)
        assert app._web_task.done()
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)


async def test_start_raises_when_web_task_returns_before_started(engine, settings, monkeypatch, caplog):
    """网站任务在标记 started 前直接返回，也属于启动失败且不输出「已启动」日志"""
    settings.web.port = free_port()

    async def quit_before_started(self, sockets=None):
        return None

    monkeypatch.setattr(EmbeddedServer, "serve", quit_before_started)
    app = Application(settings, groups={"web"}, dry_run=True)
    try:
        with caplog.at_level(logging.INFO):
            with pytest.raises(RuntimeError, match="启动前结束"):
                await asyncio.wait_for(app.start(), timeout=5)
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)
    assert not any("已启动" in r.getMessage() for r in caplog.records)


def _fake_serve(behaviour):
    """构造替代 EmbeddedServer.serve 的协程：先标记已启动，再执行 behaviour"""

    async def serve(self, sockets=None):
        self.started = True
        await behaviour(self)

    return serve


async def test_unexpected_web_exit_is_logged(engine, settings, monkeypatch, caplog):
    """网站任务在运行期间意外结束（抛异常）：记录 error 日志"""
    settings.web.port = free_port()

    async def crash(server):
        await asyncio.sleep(0.05)
        raise RuntimeError("嵌入式服务崩溃")

    monkeypatch.setattr(EmbeddedServer, "serve", _fake_serve(crash))
    app = Application(settings, groups={"web"}, dry_run=True)
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(app.start(), timeout=5)
        await wait_until(lambda: app._web_task.done())
        await asyncio.sleep(0.05)  # 让 done-callback 执行
        await asyncio.wait_for(app.stop(), timeout=5)
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("网站" in m and "嵌入式服务崩溃" in m for m in errors)


async def test_unexpected_web_return_is_logged(engine, settings, monkeypatch, caplog):
    """网站任务没有收到退出请求却正常返回：同样记录 error 日志"""
    settings.web.port = free_port()

    async def quit_quietly(server):
        await asyncio.sleep(0.05)

    monkeypatch.setattr(EmbeddedServer, "serve", _fake_serve(quit_quietly))
    app = Application(settings, groups={"web"}, dry_run=True)
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(app.start(), timeout=5)
        await wait_until(lambda: app._web_task.done())
        await asyncio.sleep(0.05)
        await asyncio.wait_for(app.stop(), timeout=5)
    assert any("网站" in r.getMessage() and r.levelno >= logging.ERROR for r in caplog.records)


async def test_cancelled_web_task_is_not_logged_as_error(engine, settings, monkeypatch, caplog):
    """网站任务被取消不算意外结束：不记 error"""
    settings.web.port = free_port()

    async def idle(server):
        await asyncio.sleep(3600)

    monkeypatch.setattr(EmbeddedServer, "serve", _fake_serve(idle))
    app = Application(settings, groups={"web"}, dry_run=True)
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(app.start(), timeout=5)
        app._web_task.cancel()
        await asyncio.gather(app._web_task, return_exceptions=True)
        await asyncio.sleep(0.05)
        await asyncio.wait_for(app.stop(), timeout=5)
    assert [r for r in caplog.records if r.name == "tidewatch.app" and r.levelno >= logging.ERROR] == []


async def test_normal_stop_does_not_log_web_error(engine, settings, caplog):
    """正常退出（stop() 通知 uvicorn 退出）不记网站相关的 error"""
    app, _ = await _start_web(settings)
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(app.stop(), timeout=5)
        await asyncio.sleep(0.05)
    assert [r for r in caplog.records if r.name == "tidewatch.app" and r.levelno >= logging.ERROR] == []


# ---------- 按组件组装配：实时更新随任一事件组与网站组合启用 ----------


async def _quiet(app: Application) -> None:
    """测试中不访问外部接口：关闭采集器 HTTP 客户端并清空采集器列表"""
    for collector in app.collectors:
        await collector.client.aclose()
    app.collectors = []


async def test_news_only_has_no_live_updates(engine, settings):
    """只开 news 组：不创建 LiveUpdates，不挂回调，也没有 live 后台任务（否则待推送队列无人消费、无限增长）"""
    app = Application(settings, groups={"news"}, dry_run=True)
    await _quiet(app)
    assert app.live is None
    await asyncio.wait_for(app.start(), timeout=5)
    try:
        assert all(getattr(l, "__self__", None).__class__.__name__ != "LiveUpdates" for l in app.sink._ingest_listeners)
        assert len(app.sink._ingest_listeners) == 1  # 只有调度器自己的回调
        assert app.health._listeners == []
        assert "live" not in [t.get_name() for t in app._tasks]
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)


async def test_web_only_has_no_live_updates(engine, settings):
    """只开 web 组：没有流水线，不创建 LiveUpdates"""
    settings.web.port = free_port()
    app = Application(settings, groups={"web"}, dry_run=True)
    assert app.live is None
    await asyncio.wait_for(app.start(), timeout=5)
    try:
        assert app.sink._ingest_listeners == [] and app.health._listeners == []
        assert "live" not in [t.get_name() for t in app._tasks]
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)


async def test_news_and_web_wire_live_updates(engine, settings):
    """news 与 web 同时开：LiveUpdates 挂到 sink 与 health 上，并启动 live 后台任务"""
    settings.web.port = free_port()
    app = Application(settings, groups={"news", "web"}, dry_run=True)
    await _quiet(app)
    assert app.live is not None
    await asyncio.wait_for(app.start(), timeout=5)
    try:
        assert app.live.on_ingest in app.sink._ingest_listeners
        assert app.live.on_health in app.health._listeners
        assert "live" in [t.get_name() for t in app._tasks]
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)


async def test_hyperliquid_only_starts_shared_pipeline_without_news_or_web(engine, settings):
    """空核心名单也能离线启动：共用流水线、健康监控和调度器，不创建新闻采集器或网站。"""
    assert settings.hyperliquid.core_wallets == []
    # M2 起有候选时按 12 小时周期等待；避免本测试触发“空名单立即真实导入”。
    async with engine.begin() as conn:
        await conn.execute(
            hl_wallets.insert().values(
                address="0x0000000000000000000000000000000000000001",
                tier="candidate",
                source="leaderboard",
            )
        )
    app = Application(settings, groups={"hyperliquid"}, dry_run=True)
    assert app.collectors == []
    assert app.live is None
    assert app._server is None
    await asyncio.wait_for(app.start(), timeout=5)
    try:
        names = {task.get_name() for task in app.critical_tasks()}
        assert {"health", "event-sink", "dispatch-route"} <= names
        assert {
            "hl-rest",
            "hl-trades-ws",
            "hl-core-ws",
            "hl-leaderboard",
            "hl-tier-recompute",
            "hl-profile",
            "hl-market-data",
        } <= names
        assert app._web_task is None
        assert {"hl_rest", "hl_trades_ws", "hl_core_ws", "hl_leaderboard", "hl_profile"} <= set(
            app.health._states
        )
        await wait_until(
            lambda: all(
                app.health.status(name) == "normal"
                for name in ("hl_rest", "hl_trades_ws", "hl_core_ws", "hl_profile")
            )
        )
        assert app.health.status("hl_leaderboard") == "unknown"
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)
    assert all(task.done() for task in app.critical_tasks())


async def test_hyperliquid_and_web_wire_live_updates_without_news(engine, settings):
    settings.web.port = free_port()
    app = Application(settings, groups={"hyperliquid", "web"}, dry_run=True)
    assert app.collectors == []
    assert app.live is not None
    await asyncio.wait_for(app.start(), timeout=5)
    try:
        assert app.live.on_ingest in app.sink._ingest_listeners
        assert app.live.on_health in app.health._listeners
        assert "live" in {task.get_name() for task in app.critical_tasks()}
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)


async def test_hyperliquid_critical_task_return_causes_nonzero_exit(engine, settings, monkeypatch, caplog):
    """Hyperliquid 长期任务正常返回也必须触发进程非零退出。"""
    from tidewatch.hyperliquid.runtime import HyperliquidRuntime

    async def return_immediately(self) -> None:
        return None

    monkeypatch.setattr(HyperliquidRuntime, "run_rest", return_immediately)
    with caplog.at_level(logging.INFO, logger="tidewatch.app"):
        code = await asyncio.wait_for(run(settings, groups={"hyperliquid"}, dry_run=True), timeout=5)
    assert code == 1
    assert any("关键任务 hl-rest 意外结束" in record.getMessage() for record in caplog.records)


# ---------- 装配：启动顺序与币安模式 ----------


async def test_start_order_health_load_then_sink_then_dispatcher(engine, settings):
    """启动顺序：health.load → sink.start → dispatcher.start（打桩只记录调用，仍执行真实实现）"""
    app = Application(settings, groups={"news"}, dry_run=True)
    await _quiet(app)
    order: list[str] = []

    def trace(owner, name: str, label: str) -> None:
        original = getattr(owner, name)

        async def wrapper(*args, **kwargs):
            order.append(label)
            return await original(*args, **kwargs)

        setattr(owner, name, wrapper)

    trace(app.health, "load", "health.load")
    trace(app.sink, "start", "sink.start")
    trace(app.dispatcher, "start", "dispatcher.start")
    await asyncio.wait_for(app.start(), timeout=5)
    try:
        assert order == ["health.load", "sink.start", "dispatcher.start"]
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)


async def _binance_mode(engine) -> str | None:
    async with engine.connect() as conn:
        return (await conn.execute(
            select(collector_state.c.mode).where(collector_state.c.name == "binance_ann")
        )).scalar_one_or_none()


async def _start_only_binance(settings) -> Application:
    """只保留币安采集器并让它的 run() 空转（不访问外部接口），启动应用"""
    app = Application(settings, groups={"news"}, dry_run=True)
    for collector in app.collectors:
        await collector.client.aclose()
    app.collectors = [c for c in app.collectors if c.name == "binance_ann"]

    async def idle() -> None:
        await asyncio.sleep(3600)

    app.collectors[0].run = idle
    await asyncio.wait_for(app.start(), timeout=5)
    return app


async def test_binance_mode_persisted_degraded_then_normal(engine, settings):
    """未配置币安 Key：collector_state.mode 落库为 degraded；之后配置 Key 重启，恢复为 normal"""
    app = await _start_only_binance(settings)
    try:
        assert isinstance(app.collectors[0], BinanceAnnPoll)
        assert await asyncio.wait_for(_binance_mode(engine), timeout=5) == "degraded"
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)
    settings.secrets.binance_api_key, settings.secrets.binance_api_secret = "k", "s"
    app = await _start_only_binance(settings)
    try:
        assert isinstance(app.collectors[0], BinanceAnnStream)
        assert await asyncio.wait_for(_binance_mode(engine), timeout=5) == "normal"
    finally:
        await asyncio.wait_for(app.stop(), timeout=5)
