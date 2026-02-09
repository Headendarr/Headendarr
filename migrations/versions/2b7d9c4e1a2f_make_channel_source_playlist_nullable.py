"""make channel_sources.playlist_id nullable and add use_hls_proxy

Revision ID: 2b7d9c4e1a2f
Revises: 3a1f6d2a6a1f
Create Date: 2026-02-08 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2b7d9c4e1a2f'
down_revision = '3a1f6d2a6a1f'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('channel_sources') as batch_op:
        batch_op.alter_column('playlist_id', existing_type=sa.Integer(), nullable=True)
        batch_op.add_column(
            sa.Column('use_hls_proxy', sa.Boolean(), nullable=False, server_default=sa.text('false'))
        )


def downgrade():
    with op.batch_alter_table('channel_sources') as batch_op:
        batch_op.drop_column('use_hls_proxy')
        batch_op.alter_column('playlist_id', existing_type=sa.Integer(), nullable=False)
