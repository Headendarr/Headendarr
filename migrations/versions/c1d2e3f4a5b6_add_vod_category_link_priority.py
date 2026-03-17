"""add priority to vod upstream category links

Revision ID: c1d2e3f4a5b6
Revises: f7c8d9e0f1a2
Create Date: 2026-03-17 13:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c1d2e3f4a5b6'
down_revision = 'f7c8d9e0f1a2'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('vod_category_xc_categories', schema=None) as batch_op:
        batch_op.add_column(sa.Column('priority', sa.Integer(), nullable=False, server_default='0'))

    op.execute(
        """
        UPDATE channel_sources
        SET priority = CASE
            WHEN trim(coalesce(priority, '')) ~ '^-?[0-9]+$' THEN trim(priority)
            ELSE '0'
        END
        """
    )
    op.alter_column(
        'channel_sources',
        'priority',
        existing_type=sa.String(length=500),
        type_=sa.Integer(),
        existing_nullable=True,
        nullable=False,
        server_default='0',
        postgresql_using="priority::integer",
    )


def downgrade():
    op.alter_column(
        'channel_sources',
        'priority',
        existing_type=sa.Integer(),
        type_=sa.String(length=500),
        existing_nullable=False,
        nullable=True,
        server_default=None,
        postgresql_using="priority::text",
    )

    with op.batch_alter_table('vod_category_xc_categories', schema=None) as batch_op:
        batch_op.drop_column('priority')
