"""add playlist hls proxy headers

Revision ID: f4a5b6c7d8e9
Revises: e2f3a4b5c6d7
Create Date: 2026-03-07 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f4a5b6c7d8e9"
down_revision = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("playlists", sa.Column("hls_proxy_headers", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("playlists", "hls_proxy_headers")
