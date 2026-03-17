"""add vod http library exposure flag

Revision ID: c6d7e8f9a0b1
Revises: c1d2e3f4a5b6
Create Date: 2026-03-17 16:40:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c6d7e8f9a0b1'
down_revision = 'c1d2e3f4a5b6'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('vod_categories', schema=None) as batch_op:
        batch_op.add_column(sa.Column('expose_http_library', sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade():
    with op.batch_alter_table('vod_categories', schema=None) as batch_op:
        batch_op.drop_column('expose_http_library')
