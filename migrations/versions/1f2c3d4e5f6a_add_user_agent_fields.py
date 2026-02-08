"""add user agent fields to playlists and epgs

Revision ID: 1f2c3d4e5f6a
Revises: 9a2a9b7c7d3e
Create Date: 2026-02-08 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1f2c3d4e5f6a'
down_revision = '9a2a9b7c7d3e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('playlists', sa.Column('user_agent', sa.String(length=255), nullable=True))
    op.add_column('epgs', sa.Column('user_agent', sa.String(length=255), nullable=True))


def downgrade():
    op.drop_column('epgs', 'user_agent')
    op.drop_column('playlists', 'user_agent')
