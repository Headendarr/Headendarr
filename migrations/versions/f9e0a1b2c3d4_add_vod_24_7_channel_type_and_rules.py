"""add vod 24/7 channel type and rules

Revision ID: f9e0a1b2c3d4
Revises: f8d9e0f1a2b3
Create Date: 2026-03-27 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "f9e0a1b2c3d4"
down_revision = "f8d9e0f1a2b3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "channels",
        sa.Column("channel_type", sa.String(length=32), nullable=False, server_default="standard"),
    )
    op.add_column("channels", sa.Column("vod_schedule_mode", sa.String(length=32), nullable=True))
    op.add_column("channels", sa.Column("vod_schedule_direction", sa.String(length=8), nullable=True))
    op.create_table(
        "vod_channel_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("channel_id", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("operator", sa.String(length=16), nullable=False, server_default="include"),
        sa.Column("rule_type", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.ForeignKeyConstraint(["channel_id"], ["channels.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_vod_channel_rules_channel_id"), "vod_channel_rules", ["channel_id"], unique=False)


def downgrade():
    op.drop_index(op.f("ix_vod_channel_rules_channel_id"), table_name="vod_channel_rules")
    op.drop_table("vod_channel_rules")
    op.drop_column("channels", "vod_schedule_direction")
    op.drop_column("channels", "vod_schedule_mode")
    op.drop_column("channels", "channel_type")
