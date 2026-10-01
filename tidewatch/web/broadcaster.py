"""进程内 SSE 广播"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator


# 放入订阅者队列的结束标记：收到后订阅者的迭代正常结束
_CLOSED = object()


class Broadcaster:
    """每个订阅者（一个 SSE 连接）一个队列；队列满时丢弃最旧的消息，避免慢连接拖垮进程

    退出时调用 close()：所有订阅者的迭代正常结束（SSE 响应随之完整收尾），之后的新订阅也立即结束。
    """

    def __init__(self, max_queue: int = 500) -> None:
        self._max_queue = max_queue
        self._subscribers: set[asyncio.Queue[Any]] = set()
        self._closed = False

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def publish(self, event: str, data: dict[str, Any]) -> None:
        if self._closed:
            return
        for queue in list(self._subscribers):
            if queue.full():
                # 整批积压已经不再可靠：清空旧消息，先通知客户端全量重同步，
                # 再尽量保留触发溢出的最新消息。
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(("resync", {}))
                if queue.full():
                    continue
            queue.put_nowait((event, data))

    def close(self) -> None:
        """结束所有订阅：向每个订阅者队列放入结束标记（队列已满时挤掉最旧的消息）；可重复调用"""
        self._closed = True
        for queue in list(self._subscribers):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(_CLOSED)

    async def subscribe(self) -> AsyncIterator[dict[str, str]]:
        """产出 sse-starlette 可直接发送的 {"event", "data"} 字典"""
        if self._closed:
            return
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._max_queue)
        self._subscribers.add(queue)
        try:
            while True:
                item = await queue.get()
                if item is _CLOSED:
                    return
                event, data = item
                yield {"event": event, "data": json.dumps(data, ensure_ascii=False, default=str)}
        finally:
            self._subscribers.discard(queue)
