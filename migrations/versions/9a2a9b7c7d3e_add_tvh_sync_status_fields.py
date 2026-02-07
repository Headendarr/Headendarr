"""add tvh sync status fields to users

Revision ID: 9a2a9b7c7d3e
Revises: 5c1b2e8b7e0a
Create Date: 2026-02-08 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9a2a9b7c7d3e'
down_revision = '5c1b2e8b7e0a'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('users', sa.Column('tvh_sync_status', sa.String(length=32), nullable=True))
    op.add_column('users', sa.Column('tvh_sync_error', sa.String(length=1024), nullable=True))
    op.add_column('users', sa.Column('tvh_sync_updated_at', sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column('users', 'tvh_sync_updated_at')
    op.drop_column('users', 'tvh_sync_error')
    op.drop_column('users', 'tvh_sync_status')
