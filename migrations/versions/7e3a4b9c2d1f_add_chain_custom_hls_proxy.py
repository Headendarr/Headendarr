"""add chain custom hls proxy option

Revision ID: 7e3a4b9c2d1f
Revises: 9c2d3a4b5f6a
Create Date: 2026-02-10 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7e3a4b9c2d1f"
down_revision = "9c2d3a4b5f6a"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "playlists",
        sa.Column(
            "chain_custom_hls_proxy",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade():
    op.drop_column("playlists", "chain_custom_hls_proxy")
