"""Alembic 迁移环境：连接串默认从 .env 的 TIDEWATCH_DATABASE_URL 读取"""

import asyncio
from logging.config import fileConfig
from pathlib import Path

from alembic import context

from tidewatch.config import load_settings
from tidewatch.store.db import create_db_engine
from tidewatch.store.tables import metadata

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata


def _database_url() -> str:
    # alembic.ini 中 sqlalchemy.url 非空时优先；否则读取仓库配置（可用环境变量 TIDEWATCH_DATABASE_URL 临时覆盖 .env）
    url = config.get_main_option("sqlalchemy.url")
    if url:
        return url
    return load_settings(Path(__file__).resolve().parents[1]).secrets.database_url


def run_migrations_offline() -> None:
    context.configure(url=_database_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_db_engine(_database_url())
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
