"""add hls proxy advanced settings

Revision ID: d8e9f0a1b2c3
Revises: c3f9a1b8d4e2
Create Date: 2026-02-17 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd8e9f0a1b2c3'
down_revision = 'c3f9a1b8d4e2'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('playlists', sa.Column('hls_proxy_use_ffmpeg', sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column('playlists', sa.Column('hls_proxy_prebuffer', sa.String(length=32), nullable=True, server_default='1M'))


def downgrade():
    op.drop_column('playlists', 'hls_proxy_prebuffer')
    op.drop_column('playlists', 'hls_proxy_use_ffmpeg')
