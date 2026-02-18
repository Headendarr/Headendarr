"""add playlist xc live stream format

Revision ID: f2c4d6e8a9b1
Revises: d8e9f0a1b2c3
Create Date: 2026-02-18 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f2c4d6e8a9b1"
down_revision = "d8e9f0a1b2c3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "playlists",
        sa.Column(
            "xc_live_stream_format",
            sa.String(length=8),
            nullable=False,
            server_default="ts",
        ),
    )


def downgrade():
    op.drop_column("playlists", "xc_live_stream_format")
