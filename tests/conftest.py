"""测试公共夹具"""

from __future__ import annotations

import os
from datetime import datetime, timezone
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
    base = dotenv_values(REPO_ROOT / ".env").get("TIDEWATCH_DATABASE_URL")
    if not base:
        return None
    return base.rsplit("/", 1)[0] + "/tidewatch_test"


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
            _schema_ready = True
        await conn.execute(
            text("TRUNCATE push_log, events, event_clusters, collector_state RESTART IDENTITY CASCADE")
        )
    yield eng
    await eng.dispose()


@pytest.fixture
def settings(tmp_path):
    """仓库默认配置：只复制 config.yaml，不合并本地 config.local.yaml；数据库指向测试库"""
    (tmp_path / "config.yaml").write_text((REPO_ROOT / "config.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    return load_settings(tmp_path, environ={"TIDEWATCH_DATABASE_URL": TEST_DB_URL or "postgresql+asyncpg://x/y"})


class FakeClock:
    """可手动推进的时钟"""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        from datetime import timedelta

        self.now = self.now + timedelta(**kwargs)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
