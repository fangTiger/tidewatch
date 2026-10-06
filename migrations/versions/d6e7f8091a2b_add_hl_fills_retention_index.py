"""为成交保留期清理增加时间索引

Revision ID: d6e7f8091a2b
Revises: c4d5e6f70819
Create Date: 2026-10-06 18:00:00.000000

"""

from typing import Sequence, Union

from alembic import op


revision: str = "d6e7f8091a2b"
down_revision: Union[str, Sequence[str], None] = "c4d5e6f70819"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """为每日按发生时间清理过期成交提供索引。"""
    op.create_index(
        "ix_hl_fills_occurred_at",
        "hl_fills",
        ["occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    """移除成交保留期清理索引。"""
    op.drop_index("ix_hl_fills_occurred_at", table_name="hl_fills")
