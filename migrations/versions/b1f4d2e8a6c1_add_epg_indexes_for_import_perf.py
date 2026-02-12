"""add epg indexes for import and build performance

Revision ID: b1f4d2e8a6c1
Revises: 8a5b2d6e7f10
Create Date: 2026-02-13 10:45:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'b1f4d2e8a6c1'
down_revision = '8a5b2d6e7f10'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('ix_epg_channels_epg_id', 'epg_channels', ['epg_id'], unique=False)
    op.create_index('ix_epg_channel_programmes_epg_channel_id', 'epg_channel_programmes', ['epg_channel_id'], unique=False)
    op.create_index(
        'ix_epg_channels_epg_id_channel_id',
        'epg_channels',
        ['epg_id', 'channel_id'],
        unique=False,
    )
    op.create_index(
        'ix_epg_channel_programmes_epg_channel_id_start',
        'epg_channel_programmes',
        ['epg_channel_id', 'start'],
        unique=False,
    )


def downgrade():
    op.drop_index('ix_epg_channel_programmes_epg_channel_id_start', table_name='epg_channel_programmes')
    op.drop_index('ix_epg_channels_epg_id_channel_id', table_name='epg_channels')
    op.drop_index('ix_epg_channel_programmes_epg_channel_id', table_name='epg_channel_programmes')
    op.drop_index('ix_epg_channels_epg_id', table_name='epg_channels')
