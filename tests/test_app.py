import asyncio
import gc
import logging
import os
import signal
import time
from types import SimpleNamespace

import httpx
import pytest

import tidewatch.app as app_module
from fakes import free_port, wait_until
from tidewatch.app import Application, parse_args, run
from tidewatch.collectors.binance_ann import BinanceAnnPoll, BinanceAnnStream
from tidewatch.collectors.jin10 import Jin10Collector
from tidewatch.collectors.okx_ann import OkxAnnCollector
from tidewatch.config import ConfigError
from tidewatch.notify.dryrun import DryRunNotifier


def test_parse_args():
    assert parse_args([]).only is None and parse_args([]).dry_run is False
    args = parse_args(["--only", "web", "--dry-run"])
    assert args.only == ["web"] and args.dry_run is True
    with pytest.raises(SystemExit):
        parse_args(["--only", "unknown"])


async def test_requires_feishu_unless_dry_run(settings):
    with pytest.raises(ConfigError) as exc:
        Application(settings, groups={"news"}, dry_run=False)
    assert "FEISHU_WEBHOOK" in str(exc.value)
    web_only = Application(settings, groups={"web"}, dry_run=False)  # 只开网站不需要飞书
    assert isinstance(web_only.notifier, DryRunNotifier) and web_only.collectors == []
    await web_only.stop()


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


async def test_run_exits_zero_on_sigterm(engine, settings):
    """收到 SIGTERM 后 run() 正常退出并返回 0（只有信号处理器装好后才发送信号，避免杀掉测试进程）"""
    settings.web.port = free_port()
    task = asyncio.create_task(run(settings, groups={"web"}, dry_run=True))
    try:
        await wait_until(lambda: signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, signal.SIG_IGN, None))
        await asyncio.sleep(0.3)  # 让应用完成启动
        os.kill(os.getpid(), signal.SIGTERM)
        assert await asyncio.wait_for(task, timeout=10) == 0
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
