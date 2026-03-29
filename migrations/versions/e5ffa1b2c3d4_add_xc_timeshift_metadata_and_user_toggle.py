"""add xc timeshift metadata and user toggle

Revision ID: e5ffa1b2c3d4
Revises: f9e0a1b2c3d4
Create Date: 2026-03-29 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "e5ffa1b2c3d4"
down_revision = "f9e0a1b2c3d4"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("playlist_streams", sa.Column("xc_epg_channel_id", sa.String(length=500), nullable=True))
    op.add_column(
        "playlist_streams",
        sa.Column("xc_tv_archive", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("playlist_streams", sa.Column("xc_tv_archive_duration", sa.Integer(), nullable=True))
    op.create_index(
        op.f("ix_playlist_streams_xc_epg_channel_id"), "playlist_streams", ["xc_epg_channel_id"], unique=False
    )

    op.add_column("users", sa.Column("timeshift_enabled", sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade():
    op.drop_column("users", "timeshift_enabled")

    op.drop_index(op.f("ix_playlist_streams_xc_epg_channel_id"), table_name="playlist_streams")
    op.drop_column("playlist_streams", "xc_tv_archive_duration")
    op.drop_column("playlist_streams", "xc_tv_archive")
    op.drop_column("playlist_streams", "xc_epg_channel_id")
