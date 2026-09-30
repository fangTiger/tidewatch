"""日志初始化

日志按天写入 log/tidewatch_YYYY-MM-DD.log（按上海时区换日），保留 N 天，同时输出到标准输出。
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, TextIO

from tidewatch.clock import LOCAL_TZ
from tidewatch.config import LogCfg

_FORMAT = "%(asctime)s %(levelname)s %(name)s：%(message)s"


def _local_today() -> date:
    return datetime.now(LOCAL_TZ).date()


class DailyFileHandler(logging.Handler):
    """按天切换日志文件的处理器，文件名形如 {prefix}_YYYY-MM-DD.log"""

    def __init__(
        self,
        directory: Path | str,
        prefix: str,
        retention_days: int,
        today: Callable[[], date] = _local_today,
    ) -> None:
        super().__init__()
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._prefix = prefix
        self._retention_days = retention_days
        self._today = today
        self._current: date | None = None
        self._stream: TextIO | None = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            today = self._today()
            if today != self._current:
                self._rollover(today)
            stream = self._stream
            if stream is None:
                raise RuntimeError("日志文件尚未打开")
            stream.write(self.format(record) + "\n")
            stream.flush()
        except Exception:
            self.handleError(record)

    def _rollover(self, today: date) -> None:
        # 先打开新文件：打开失败时异常直接抛出，旧流与 _current 保持不变，下次 emit 会重试换日
        new_stream = open(self._dir / f"{self._prefix}_{today.isoformat()}.log", "a", encoding="utf-8")
        old_stream = self._stream
        self._stream = new_stream
        self._current = today
        if old_stream is not None:
            old_stream.close()
        self._cleanup(today)

    def _cleanup(self, today: date) -> None:
        """删除超过保留天数的日志文件（保留今天在内的 retention_days 天）"""
        cutoff = today - timedelta(days=self._retention_days - 1)
        for path in self._dir.glob(f"{self._prefix}_*.log"):
            try:
                day = date.fromisoformat(path.stem[len(self._prefix) + 1 :])
            except ValueError:
                continue
            if day < cutoff:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    # 清理旧日志失败不应影响当前日志写入，下次换日再试
                    continue

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        super().close()


def setup_logging(cfg: LogCfg, base_dir: Path | str) -> None:
    """配置根日志：按天文件 + 标准输出"""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(cfg.level)
    formatter = logging.Formatter(_FORMAT)
    # 时间戳统一用上海时间，与日志文件的换日时区一致
    formatter.converter = lambda ts: datetime.fromtimestamp(ts, LOCAL_TZ).timetuple()
    file_handler = DailyFileHandler(Path(base_dir) / cfg.dir, "tidewatch", cfg.retention_days)
    stream_handler = logging.StreamHandler(sys.stdout)
    for handler in (file_handler, stream_handler):
        handler.setFormatter(formatter)
        root.addHandler(handler)
    # 第三方库的请求日志过于频繁，只保留警告以上
    for noisy in ("httpx", "httpcore", "websockets", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
