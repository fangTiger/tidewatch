"""数据库引擎"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_db_engine(url: str, *, echo: bool = False) -> AsyncEngine:
    """创建异步引擎（asyncpg 驱动），连接串形如 postgresql+asyncpg://用户:密码@主机:端口/库名"""
    return create_async_engine(url, pool_size=5, max_overflow=5, pool_pre_ping=True, echo=echo)
