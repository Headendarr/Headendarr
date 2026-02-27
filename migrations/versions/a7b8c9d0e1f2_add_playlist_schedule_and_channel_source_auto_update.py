"""add playlist schedule and channel source auto update

Revision ID: a7b8c9d0e1f2
Revises: e1b2c3d4e5f6
Create Date: 2026-02-27 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a7b8c9d0e1f2"
down_revision = "e1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "playlists",
        sa.Column("update_schedule", sa.String(length=16), nullable=False, server_default="off"),
    )
    op.alter_column("playlists", "update_schedule", server_default=None)

    op.add_column(
        "channel_sources",
        sa.Column("auto_update", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.alter_column("channel_sources", "auto_update", server_default=None)


def downgrade():
    op.drop_column("channel_sources", "auto_update")
    op.drop_column("playlists", "update_schedule")
