"""add channel source health check fields

Revision ID: c9d8e7f6a5b4
Revises: b8c9d0e1f2a3
Create Date: 2026-03-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c9d8e7f6a5b4"
down_revision = "b8c9d0e1f2a3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "channel_sources",
        sa.Column("last_health_check_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "channel_sources",
        sa.Column("last_health_check_status", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "channel_sources",
        sa.Column("last_health_check_reason", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "channel_sources",
        sa.Column("last_health_check_metrics", sa.Text(), nullable=True),
    )


def downgrade():
    op.drop_column("channel_sources", "last_health_check_metrics")
    op.drop_column("channel_sources", "last_health_check_reason")
    op.drop_column("channel_sources", "last_health_check_status")
    op.drop_column("channel_sources", "last_health_check_at")
