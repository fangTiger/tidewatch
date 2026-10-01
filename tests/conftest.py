"""测试公共夹具"""

from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from dotenv import dotenv_values
from sqlalchemy import text
from sqlalchemy.engine import make_url

from tidewatch.config import load_settings
from tidewatch.store.db import create_db_engine
from tidewatch.store.tables import metadata

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _test_db_url() -> str | None:
    url = os.environ.get("TIDEWATCH_TEST_DATABASE_URL")
    if url:
        return url
    # 环境变量优先，其次 .env
    base = os.environ.get("TIDEWATCH_DATABASE_URL") or dotenv_values(REPO_ROOT / ".env").get(
        "TIDEWATCH_DATABASE_URL"
    )
    if not base:
        return None
    # 只替换库名，保留用户名、密码、主机与查询参数
    return make_url(base).set(database="tidewatch_test").render_as_string(hide_password=False)


TEST_DB_URL = _test_db_url()
_schema_ready = False


@pytest.fixture
async def engine():
    """每个测试一个引擎；首次使用时重建表结构，之后每次清空数据"""
    global _schema_ready
    if TEST_DB_URL is None:
        pytest.skip("未配置测试数据库：请设置 TIDEWATCH_TEST_DATABASE_URL 或 .env 中的 TIDEWATCH_DATABASE_URL")
    # 防止误把正式库当测试库：库名必须以 _test 结尾
    assert make_url(TEST_DB_URL).database.endswith("_test"), "测试库名必须以 _test 结尾"
    eng = create_db_engine(TEST_DB_URL)
    async with eng.begin() as conn:
        if not _schema_ready:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)
        # 表名由元数据动态生成，新增表后无需再改夹具
        tables = ", ".join(t.name for t in reversed(metadata.sorted_tables))
        await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
    # 事务成功提交后才标记表结构已就绪，避免失败后后续测试跳过建表
    _schema_ready = True
    yield eng
    await eng.dispose()


@pytest.fixture
def settings(tmp_path):
    """仓库默认配置：只复制 config.yaml，不合并本地 config.local.yaml；数据库指向测试库"""
    (tmp_path / "config.yaml").write_text((REPO_ROOT / "config.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    return load_settings(tmp_path, environ={"TIDEWATCH_DATABASE_URL": TEST_DB_URL or "postgresql+asyncpg://x/y"})


@pytest.fixture(autouse=True)
def _reset_sse_app_status():
    """sse-starlette 的退出标记是进程全局的：任何一次 uvicorn 退出都会让之后所有 SSE 流立即结束，每个测试前后复位"""
    from sse_starlette.sse import AppStatus

    AppStatus.should_exit = False
    yield
    AppStatus.should_exit = False


@pytest.fixture
def occupied_port():
    """一个已被占用（正在监听）的本机端口；用完关闭"""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    try:
        yield sock.getsockname()[1]
    finally:
        sock.close()


class FakeClock:
    """可手动推进的时钟"""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now = self.now + timedelta(**kwargs)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
