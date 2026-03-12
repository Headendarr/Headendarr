"""add channel source stream probe fields

Revision ID: f5a6b7c8d9e0
Revises: d2a4b6c8e0f1
Create Date: 2026-03-11 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f5a6b7c8d9e0"
down_revision = "d2a4b6c8e0f1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "channel_sources",
        sa.Column("stream_probe_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "channel_sources",
        sa.Column("stream_probe_details", sa.Text(), nullable=True),
    )


def downgrade():
    op.drop_column("channel_sources", "stream_probe_details")
    op.drop_column("channel_sources", "stream_probe_at")
