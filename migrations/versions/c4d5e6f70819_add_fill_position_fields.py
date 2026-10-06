"""为画像持仓轮次保存成交前仓位与方向

Revision ID: c4d5e6f70819
Revises: 7b1c2d3e4f50
Create Date: 2026-10-06 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c4d5e6f70819"
down_revision: Union[str, Sequence[str], None] = "7b1c2d3e4f50"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """增加可复算 0→持仓→0 轮次所需字段。"""
    op.add_column("hl_fills", sa.Column("side", sa.Text(), nullable=True))
    op.add_column("hl_fills", sa.Column("start_position", sa.Numeric(), nullable=True))


def downgrade() -> None:
    """删除画像轮次辅助字段。"""
    op.drop_column("hl_fills", "start_position")
    op.drop_column("hl_fills", "side")
