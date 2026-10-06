"""创建二期 Hyperliquid 专用表

Revision ID: 7b1c2d3e4f50
Revises: 8f3bbaee6ef9
Create Date: 2026-10-06 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# Alembic revision 标识。
revision: str = "7b1c2d3e4f50"
down_revision: Union[str, Sequence[str], None] = "8f3bbaee6ef9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """创建二期 Hyperliquid 钱包、仓位、成交和游标表。"""
    op.create_table(
        "hl_wallets",
        sa.Column("address", sa.Text(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("tier", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("excluded", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("style", sa.Text(), nullable=True),
        sa.Column(
            "profile",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("profile_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_position_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("address"),
    )
    op.create_index(
        "ix_hl_wallets_tier_excluded",
        "hl_wallets",
        ["tier", "excluded"],
        unique=False,
    )
    op.create_index(
        "ix_hl_wallets_last_position_at",
        "hl_wallets",
        ["last_position_at"],
        unique=False,
    )

    op.create_table(
        "hl_positions",
        sa.Column("address", sa.Text(), nullable=False),
        sa.Column("dex", sa.Text(), nullable=False),
        sa.Column("coin", sa.Text(), nullable=False),
        sa.Column("szi", sa.Numeric(), nullable=False),
        sa.Column("entry_px", sa.Numeric(), nullable=True),
        sa.Column("liquidation_px", sa.Numeric(), nullable=True),
        sa.Column("leverage", sa.Numeric(), nullable=True),
        sa.Column("notional_usd", sa.Numeric(), nullable=True),
        sa.Column("unrealized_pnl", sa.Numeric(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("address", "dex", "coin"),
    )
    op.create_index("ix_hl_positions_coin", "hl_positions", ["coin"], unique=False)
    op.create_index(
        "ix_hl_positions_updated_at",
        "hl_positions",
        ["updated_at"],
        unique=False,
    )

    op.create_table(
        "hl_fills",
        sa.Column("address", sa.Text(), nullable=False),
        sa.Column("tid", sa.BigInteger(), nullable=False),
        sa.Column("coin", sa.Text(), nullable=False),
        sa.Column("dir", sa.Text(), nullable=False),
        sa.Column("px", sa.Numeric(), nullable=False),
        sa.Column("sz", sa.Numeric(), nullable=False),
        sa.Column("closed_pnl", sa.Numeric(), nullable=False),
        sa.Column("fee", sa.Numeric(), nullable=False),
        sa.Column("crossed", sa.Boolean(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("hash", sa.Text(), nullable=False),
        sa.Column("is_twap", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("liquidation", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("address", "tid"),
    )
    op.create_index(
        "ix_hl_fills_address_occurred_at",
        "hl_fills",
        ["address", "occurred_at"],
        unique=False,
    )
    op.create_index(
        "ix_hl_fills_coin_occurred_at",
        "hl_fills",
        ["coin", "occurred_at"],
        unique=False,
    )

    op.create_table(
        "hl_state",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column(
            "cursor",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("name"),
    )


def downgrade() -> None:
    """仅删除二期 Hyperliquid 表，保留一期表与数据。"""
    op.drop_table("hl_state")
    op.drop_index("ix_hl_fills_coin_occurred_at", table_name="hl_fills")
    op.drop_index("ix_hl_fills_address_occurred_at", table_name="hl_fills")
    op.drop_table("hl_fills")
    op.drop_index("ix_hl_positions_updated_at", table_name="hl_positions")
    op.drop_index("ix_hl_positions_coin", table_name="hl_positions")
    op.drop_table("hl_positions")
    op.drop_index("ix_hl_wallets_last_position_at", table_name="hl_wallets")
    op.drop_index("ix_hl_wallets_tier_excluded", table_name="hl_wallets")
    op.drop_table("hl_wallets")
