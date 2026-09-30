import logging
from datetime import date

from tidewatch import logging_setup
from tidewatch.logging_setup import DailyFileHandler


def _record(message: str) -> logging.LogRecord:
    return logging.LogRecord("t", logging.INFO, __file__, 1, message, None, None)


def test_daily_file_naming_and_cleanup(tmp_path):
    (tmp_path / "tidewatch_2026-09-01.log").write_text("old", encoding="utf-8")
    (tmp_path / "tidewatch_2026-09-20.log").write_text("keep", encoding="utf-8")
    today = {"value": date(2026, 9, 30)}
    handler = DailyFileHandler(tmp_path, "tidewatch", retention_days=14, today=lambda: today["value"])
    handler.setFormatter(logging.Formatter("%(message)s"))

    handler.emit(_record("第一条日志"))
    assert (tmp_path / "tidewatch_2026-09-30.log").read_text(encoding="utf-8") == "第一条日志\n"
    assert not (tmp_path / "tidewatch_2026-09-01.log").exists()
    assert (tmp_path / "tidewatch_2026-09-20.log").exists()

    today["value"] = date(2026, 10, 1)
    handler.emit(_record("第二天"))
    assert (tmp_path / "tidewatch_2026-10-01.log").read_text(encoding="utf-8") == "第二天\n"
    handler.close()


def _flaky_open(monkeypatch, failures: int):
    """让 logging_setup 模块内的 open 前 failures 次抛 OSError，之后恢复正常"""
    real_open = open
    state = {"calls": 0}

    def fake_open(*args, **kwargs):
        state["calls"] += 1
        if state["calls"] <= failures:
            raise OSError("磁盘暂时不可用")
        return real_open(*args, **kwargs)

    monkeypatch.setattr(logging_setup, "open", fake_open, raising=False)


def test_rollover_open_failure_keeps_old_state_and_recovers(tmp_path, monkeypatch):
    today = {"value": date(2026, 9, 30)}
    handler = DailyFileHandler(tmp_path, "tidewatch", retention_days=14, today=lambda: today["value"])
    handler.setFormatter(logging.Formatter("%(message)s"))
    errors: list[logging.LogRecord] = []
    handler.handleError = errors.append  # 记录内部错误，避免向 stderr 打印

    handler.emit(_record("换日前"))

    # 换日时打开新文件失败：旧文件流必须保持可用，不能被提前关闭
    today["value"] = date(2026, 10, 1)
    _flaky_open(monkeypatch, failures=1)
    handler.emit(_record("失败的一条"))
    assert len(errors) == 1
    assert handler._stream is not None and not handler._stream.closed

    # 故障恢复后的下一次 emit 应重试换日并正常写入新一天的文件
    handler.emit(_record("恢复后"))
    assert len(errors) == 1
    assert (tmp_path / "tidewatch_2026-10-01.log").read_text(encoding="utf-8") == "恢复后\n"
    assert (tmp_path / "tidewatch_2026-09-30.log").read_text(encoding="utf-8") == "换日前\n"
    handler.close()


def test_first_open_failure_recovers_on_next_emit(tmp_path, monkeypatch):
    handler = DailyFileHandler(tmp_path, "tidewatch", retention_days=14, today=lambda: date(2026, 9, 30))
    handler.setFormatter(logging.Formatter("%(message)s"))
    errors: list[logging.LogRecord] = []
    handler.handleError = errors.append

    _flaky_open(monkeypatch, failures=1)
    handler.emit(_record("失败的一条"))
    assert len(errors) == 1

    handler.emit(_record("恢复后"))
    assert len(errors) == 1
    assert (tmp_path / "tidewatch_2026-09-30.log").read_text(encoding="utf-8") == "恢复后\n"
    handler.close()


def test_setup_logging_writes_file_and_stdout(tmp_path):
    import sys

    from tidewatch.config import LogCfg
    from tidewatch.logging_setup import setup_logging

    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    # setup_logging 会调整这几个第三方 logger 的级别，测试结束后需要恢复
    noisy_names = ("httpx", "httpcore", "websockets", "uvicorn.access")
    saved_levels = {name: logging.getLogger(name).level for name in noisy_names}
    try:
        setup_logging(LogCfg(dir="log", retention_days=14, level="INFO"), tmp_path)
        logging.getLogger("tidewatch.test").info("中文日志")
        files = list((tmp_path / "log").glob("tidewatch_*.log"))
        assert len(files) == 1 and "中文日志" in files[0].read_text(encoding="utf-8")
        assert any(isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout for h in root.handlers)
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        for handler in saved_handlers:
            root.addHandler(handler)
        root.setLevel(saved_level)
        for name, level in saved_levels.items():
            logging.getLogger(name).setLevel(level)
