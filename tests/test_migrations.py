"""Alembic 真实迁移测试。"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from conftest import TEST_DB_URL
from tidewatch.store.tables import metadata

TABLES = {"collector_state", "event_clusters", "events", "push_log"}
NAMED_INDEXES = {
    "event_clusters": {"ix_event_clusters_category_first_seen"},
    "events": {"ix_events_cluster_id", "ix_events_collected_at", "ix_events_occurred_at"},
    "push_log": {"ix_push_log_cluster_id", "ix_push_log_status"},
}


def test_process_runner_enforces_timeout():
    """同步网络操作超时后必须终止子进程，不能拖住测试进程。"""
    started_at = time.monotonic()
    with pytest.raises(TimeoutError, match="操作超时"):
        _run_in_process(time.sleep, 5, timeout=0.05)
    assert time.monotonic() - started_at < 2


def _process_entry(sender, target: Callable[..., Any], args: tuple[Any, ...]) -> None:
    """在子进程中执行同步操作，并把异常回传给父进程。"""
    try:
        target(*args)
    except BaseException:
        sender.send(traceback.format_exc())
    else:
        sender.send(None)
    finally:
        sender.close()


def _run_in_process(target: Callable[..., Any], *args: Any, timeout: float) -> None:
    """在可强制终止的子进程中执行同步操作。"""
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_process_entry, args=(sender, target, args), daemon=True)
    process.start()
    sender.close()
    process.join(timeout)
    if process.is_alive():
        process.terminate()
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join()
        receiver.close()
        raise TimeoutError(f"操作超时（{timeout} 秒）")
    try:
        error = receiver.recv() if receiver.poll() else None
    finally:
        receiver.close()
    if error is not None:
        raise RuntimeError(f"子进程操作失败：\n{error}")
    if process.exitcode != 0:
        raise RuntimeError(f"子进程异常退出，退出码：{process.exitcode}")


def _run_alembic(action: str, database_url: str) -> None:
    """在子进程内以编程方式执行 Alembic 命令。"""
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    if action == "upgrade":
        command.upgrade(config, "head")
    elif action == "downgrade":
        command.downgrade(config, "base")
    else:
        raise ValueError(f"不支持的 Alembic 操作：{action}")


def _run(coro: Awaitable[object]) -> object:
    """为测试中的异步数据库操作设置超时。"""
    return asyncio.run(asyncio.wait_for(coro, timeout=30))


async def _create_database(maintenance_url: str, database: str) -> None:
    engine = create_async_engine(maintenance_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{database}"'))
    finally:
        await engine.dispose()


async def _drop_database(maintenance_url: str, database: str) -> None:
    engine = create_async_engine(maintenance_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database AND pid <> pg_backend_pid()"
                ),
                {"database": database},
            )
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{database}"'))
    finally:
        await engine.dispose()


async def _inspect_upgraded_database(database_url: str) -> tuple[set[str], dict[str, list[dict]], list]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            def inspect_schema(sync_conn):
                inspector = inspect(sync_conn)
                tables = set(inspector.get_table_names())
                indexes = {table: inspector.get_indexes(table) for table in NAMED_INDEXES}
                context = MigrationContext.configure(sync_conn)
                differences = compare_metadata(context, metadata)
                return tables, indexes, differences

            return await conn.run_sync(inspect_schema)
    finally:
        await engine.dispose()


async def _table_names(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            return await conn.run_sync(lambda sync_conn: set(inspect(sync_conn).get_table_names()))
    finally:
        await engine.dispose()


@pytest.mark.skipif(TEST_DB_URL is None, reason="未配置测试数据库")
def test_upgrade_head_matches_metadata_and_downgrade_base_removes_tables():
    """空库升级后结构与元数据一致，降级后一期四张表全部删除。"""
    application_logger = logging.getLogger("tidewatch.migration_test_probe")
    application_logger.disabled = False
    base_url = make_url(TEST_DB_URL)
    assert base_url.database and base_url.database.endswith("_test"), "测试库名必须以 _test 结尾"
    database = f"tidewatch_migration_{uuid.uuid4().hex}_test"
    maintenance_url = base_url.set(database="postgres").render_as_string(hide_password=False)
    database_url = base_url.set(database=database).render_as_string(hide_password=False)
    created = False
    try:
        _run(_create_database(maintenance_url, database))
        created = True
        _run_in_process(_run_alembic, "upgrade", database_url, timeout=30)

        tables, indexes, differences = _run(_inspect_upgraded_database(database_url))
        assert TABLES <= tables
        for table, expected in NAMED_INDEXES.items():
            assert expected <= {index["name"] for index in indexes[table]}
        assert any(
            index["unique"] and index["column_names"] == ["dedupe_key"]
            for index in indexes["events"]
        )
        assert differences == []

        _run_in_process(_run_alembic, "downgrade", database_url, timeout=30)
        assert TABLES.isdisjoint(_run(_table_names(database_url)))
        assert application_logger.disabled is False
    finally:
        if created:
            _run(_drop_database(maintenance_url, database))
