"""嵌入应用事件循环的 uvicorn 服务器"""

from __future__ import annotations

import contextlib

import uvicorn


class EmbeddedServer(uvicorn.Server):
    """不接管进程信号的嵌入式 uvicorn 服务器"""

    @contextlib.contextmanager
    def capture_signals(self):
        """保留现有信号处理器，由外层应用统一处理退出信号"""
        yield

    async def serve(self, sockets=None) -> None:
        """运行 uvicorn，并把其启动时的进程退出转换为普通异常"""
        try:
            await super().serve(sockets=sockets)
        except SystemExit as exc:
            host, port = self.config.host, self.config.port
            raise RuntimeError(f"网站启动失败：无法监听 {host}:{port}（端口可能被占用）") from exc
