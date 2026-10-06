"""Hyperliquid WebSocket 连接与订阅管理。"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from websockets.asyncio.client import connect as websocket_connect

from tidewatch.collectors.base import Backoff
from tidewatch.hyperliquid.logging import exception_summary

logger = logging.getLogger(__name__)

WS_URL = "wss://api.hyperliquid.xyz/ws"
MAX_CONNECTIONS = 10
MAX_SUBSCRIPTIONS = 1_000
MAX_USERS = 10
STABLE_CONNECTION_S = 60.0

Sleep = Callable[[float], Awaitable[None]]
MessageHandler = Callable[[Any], Awaitable[bool | None] | bool | None]
ConnectHandler = Callable[[], Awaitable[None] | None]


@dataclass
class _Connection:
    handler: MessageHandler | None = None
    on_connect: ConnectHandler | None = None
    health_source: str = "hl_core_ws"
    subscriptions: dict[str, dict[str, Any]] = field(default_factory=dict)


class HyperliquidWsManager:
    """集中维护 WS 连接，负责心跳、退避重连与订阅恢复。"""

    def __init__(
        self,
        *,
        proxy_mode: str = "global",
        proxy_url: str | None = None,
        health: Any = None,
        url: str = WS_URL,
        sleep: Sleep = asyncio.sleep,
        ping_interval_s: float = 50.0,
        connect: Callable[..., Any] = websocket_connect,
        health_source: str = "hl_core_ws",
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if proxy_mode not in {"global", "direct"}:
            raise ValueError("Hyperliquid 代理模式必须是 global 或 direct")
        if ping_interval_s <= 0:
            raise ValueError("Hyperliquid WS 心跳间隔必须大于 0")
        self.proxy_mode = proxy_mode
        self._proxy = proxy_url if proxy_mode == "global" and proxy_url else None
        self.health = health
        self.url = url
        self._sleep = sleep
        self._ping_interval_s = ping_interval_s
        self._connect = connect
        self._monotonic = monotonic
        self._default_health_source = health_source
        self._connections: dict[str, _Connection] = {}
        self._active: dict[str, Any] = {}
        self._connection_changed = asyncio.Event()
        self._closed = asyncio.Event()
        self._running = False

    @property
    def subscription_count(self) -> int:
        return sum(len(connection.subscriptions) for connection in self._connections.values())

    def add_connection(
        self,
        name: str,
        handler: MessageHandler | None = None,
        *,
        health_source: str | None = None,
        on_connect: ConnectHandler | None = None,
    ) -> bool:
        """登记一个逻辑连接；同名登记为幂等操作。"""
        if name in self._connections:
            return False
        if len(self._connections) >= MAX_CONNECTIONS:
            raise ValueError("Hyperliquid WS 每个出口 IP 最多 10 个连接")
        self._connections[name] = _Connection(
            handler=handler,
            on_connect=on_connect,
            health_source=health_source or self._default_health_source,
        )
        self._connection_changed.set()
        return True

    def add_subscription(self, connection_name: str, subscription: Mapping[str, Any]) -> bool:
        """登记订阅；重复订阅不重复计数或发送。"""
        connection = self._connection(connection_name)
        normalized = dict(subscription)
        key = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if key in connection.subscriptions:
            return False
        if self.subscription_count >= MAX_SUBSCRIPTIONS:
            raise ValueError("Hyperliquid WS 总订阅最多 1,000 个")
        user = self._subscription_user(normalized)
        users = self._subscribed_users()
        if user is not None and user not in users and len(users) >= MAX_USERS:
            raise ValueError("Hyperliquid WS 按用户订阅最多 10 个用户")
        connection.subscriptions[key] = normalized
        return True

    async def subscribe(self, connection_name: str, subscription: Mapping[str, Any]) -> bool:
        """登记订阅；连接已建立时立即发送。"""
        added = self.add_subscription(connection_name, subscription)
        websocket = self._active.get(connection_name)
        if added and websocket is not None:
            await self._send_dynamic(
                connection_name,
                websocket,
                self._subscribe_message(dict(subscription)),
            )
        return added

    async def unsubscribe(self, connection_name: str, subscription: Mapping[str, Any]) -> bool:
        """移除订阅；连接已建立时发送协议退订消息。"""
        connection = self._connection(connection_name)
        normalized = dict(subscription)
        key = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if key not in connection.subscriptions:
            return False
        del connection.subscriptions[key]
        websocket = self._active.get(connection_name)
        if websocket is not None:
            await self._send_dynamic(
                connection_name,
                websocket,
                self._unsubscribe_message(normalized),
            )
        return True

    async def _send_dynamic(self, name: str, websocket: Any, message: str) -> None:
        """断线竞态下保留期望订阅集合，等待连接管理循环恢复。"""
        try:
            await websocket.send(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            connection = self._connection(name)
            await self._report_failure(connection.health_source, exc)
            logger.warning(
                "WS 连接 %s 动态订阅发送失败，将在重连后恢复：%s",
                name,
                exception_summary(exc),
            )

    async def run(self) -> None:
        """运行全部逻辑连接，并接纳运行期间新增的连接。"""
        if self._running:
            raise RuntimeError("Hyperliquid WS 管理器已经在运行")
        self._running = True
        tasks: dict[str, asyncio.Task[None]] = {}
        try:
            while not self._closed.is_set():
                for name, connection in self._connections.items():
                    if name not in tasks:
                        tasks[name] = asyncio.create_task(
                            self._run_connection(name, connection),
                            name=f"hl-ws-{name}",
                        )
                self._connection_changed.clear()
                changed = asyncio.create_task(self._connection_changed.wait())
                closed = asyncio.create_task(self._closed.wait())
                done, pending = await asyncio.wait(
                    (*tasks.values(), changed, closed),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for waiter in (changed, closed):
                    if waiter not in done:
                        waiter.cancel()
                await asyncio.gather(changed, closed, return_exceptions=True)
                if closed in done:
                    return
                for name, task in list(tasks.items()):
                    if task in done:
                        await task
                        tasks.pop(name, None)
        finally:
            for task in tasks.values():
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            self._running = False

    async def _run_connection(self, name: str, connection: _Connection) -> None:
        backoff = Backoff()
        while not self._closed.is_set():
            heartbeat: asyncio.Task[None] | None = None
            connected_at: float | None = None
            try:
                async with self._connect(
                    self.url,
                    proxy=self._proxy,
                    ping_interval=None,
                    close_timeout=1,
                ) as websocket:
                    connected_at = self._monotonic()
                    self._active[name] = websocket
                    if self._closed.is_set():
                        await websocket.close()
                        return
                    for subscription in list(connection.subscriptions.values()):
                        await websocket.send(self._subscribe_message(subscription))
                    if connection.on_connect is not None:
                        result = connection.on_connect()
                        if inspect.isawaitable(result):
                            await result
                    heartbeat = asyncio.create_task(self._heartbeat(websocket), name=f"hl-ws-{name}-heartbeat")
                    async for raw_message in websocket:
                        message = json.loads(raw_message)
                        if self._monotonic() - connected_at >= STABLE_CONNECTION_S:
                            backoff.reset()
                        if connection.handler is not None:
                            result = connection.handler(message)
                            if inspect.isawaitable(result):
                                result = await result
                            if result is False:
                                raise ValueError("WebSocket 消息未通过业务校验")
                        await self._report_success(connection.health_source)
                    if not self._closed.is_set():
                        raise ConnectionError("WebSocket 连接已关闭")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._closed.is_set():
                    return
                await self._report_failure(connection.health_source, exc)
                if connected_at is not None and self._monotonic() - connected_at >= STABLE_CONNECTION_S:
                    backoff.reset()
                delay = backoff.next()
                logger.warning(
                    "子源 %s 的 WS 连接 %s 断开：%s；%g 秒后重连，代理模式 %s",
                    connection.health_source,
                    name,
                    exception_summary(exc),
                    delay,
                    self.proxy_mode,
                )
                if await self._sleep_or_closed(delay):
                    return
            finally:
                self._active.pop(name, None)
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)

    async def _heartbeat(self, websocket: Any) -> None:
        while True:
            await self._sleep(self._ping_interval_s)
            await websocket.send('{"method":"ping"}')

    async def _sleep_or_closed(self, delay: float) -> bool:
        """等待重连间隔；关闭事件可立即打断等待。"""
        sleeper = asyncio.create_task(self._sleep(delay))
        closer = asyncio.create_task(self._closed.wait())
        done, pending = await asyncio.wait((sleeper, closer), return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            if task is sleeper:
                await task
        return closer in done

    async def _report_success(self, source: str) -> None:
        if self.health is None:
            return
        try:
            await self.health.success(source)
        except Exception as exc:
            logger.error("子源 %s 健康成功状态上报失败：%s", source, exception_summary(exc))

    async def _report_failure(self, source: str, exc: BaseException) -> None:
        if self.health is None:
            return
        try:
            await self.health.failure(source, exception_summary(exc))
        except Exception as report_exc:
            logger.error(
                "子源 %s 健康失败状态上报失败：%s",
                source,
                exception_summary(report_exc),
            )

    async def aclose(self) -> None:
        """唤醒等待并主动关闭在途连接，使接收循环及时退出。"""
        self._closed.set()
        active = list(self._active.values())
        if active:
            await asyncio.gather(
                *(websocket.close() for websocket in active),
                return_exceptions=True,
            )

    close = aclose

    def _connection(self, name: str) -> _Connection:
        try:
            return self._connections[name]
        except KeyError as exc:
            raise ValueError(f"Hyperliquid WS 连接未登记：{name}") from exc

    def _subscribed_users(self) -> set[str]:
        users: set[str] = set()
        for connection in self._connections.values():
            for subscription in connection.subscriptions.values():
                user = self._subscription_user(subscription)
                if user is not None:
                    users.add(user)
        return users

    @staticmethod
    def _subscription_user(subscription: Mapping[str, Any]) -> str | None:
        user = subscription.get("user")
        return user.casefold() if isinstance(user, str) and user else None

    @staticmethod
    def _subscribe_message(subscription: Mapping[str, Any]) -> str:
        return json.dumps(
            {"method": "subscribe", "subscription": dict(subscription)},
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _unsubscribe_message(subscription: Mapping[str, Any]) -> str:
        return json.dumps(
            {"method": "unsubscribe", "subscription": dict(subscription)},
            ensure_ascii=False,
            separators=(",", ":"),
        )
