import logging
from datetime import date

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


def test_setup_logging_writes_file_and_stdout(tmp_path):
    import sys

    from tidewatch.config import LogCfg
    from tidewatch.logging_setup import setup_logging

    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
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
