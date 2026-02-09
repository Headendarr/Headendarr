"""add dvr tables and user agent fields

Revision ID: 3a1f6d2a6a1f
Revises: 9a2a9b7c7d3e
Create Date: 2026-02-08 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3a1f6d2a6a1f'
down_revision = '9a2a9b7c7d3e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('playlists', sa.Column('user_agent', sa.String(length=255), nullable=True))
    op.add_column('epgs', sa.Column('user_agent', sa.String(length=255), nullable=True))

    op.create_table(
        'recording_rules',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('channel_id', sa.Integer(), sa.ForeignKey('channels.id'), nullable=False),
        sa.Column('title_match', sa.String(length=500), nullable=True),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('lookahead_days', sa.Integer(), nullable=False, server_default='7'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_recording_rules_channel_id', 'recording_rules', ['channel_id'])
    op.create_index('ix_recording_rules_title_match', 'recording_rules', ['title_match'])

    op.create_table(
        'recordings',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('channel_id', sa.Integer(), sa.ForeignKey('channels.id'), nullable=False),
        sa.Column('rule_id', sa.Integer(), sa.ForeignKey('recording_rules.id'), nullable=True),
        sa.Column('epg_programme_id', sa.Integer(), nullable=True),
        sa.Column('title', sa.String(length=500), nullable=True),
        sa.Column('description', sa.String(length=2000), nullable=True),
        sa.Column('start_ts', sa.Integer(), nullable=True),
        sa.Column('stop_ts', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=True),
        sa.Column('sync_status', sa.String(length=32), nullable=True),
        sa.Column('sync_error', sa.String(length=1024), nullable=True),
        sa.Column('tvh_uuid', sa.String(length=128), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_recordings_channel_id', 'recordings', ['channel_id'])
    op.create_index('ix_recordings_start_ts', 'recordings', ['start_ts'])
    op.create_index('ix_recordings_stop_ts', 'recordings', ['stop_ts'])
    op.create_index('ix_recordings_status', 'recordings', ['status'])
    op.create_index('ix_recordings_sync_status', 'recordings', ['sync_status'])
    op.create_index('ix_recordings_tvh_uuid', 'recordings', ['tvh_uuid'])


def downgrade():
    op.drop_column('epgs', 'user_agent')
    op.drop_column('playlists', 'user_agent')

    op.drop_index('ix_recordings_tvh_uuid', table_name='recordings')
    op.drop_index('ix_recordings_sync_status', table_name='recordings')
    op.drop_index('ix_recordings_status', table_name='recordings')
    op.drop_index('ix_recordings_stop_ts', table_name='recordings')
    op.drop_index('ix_recordings_start_ts', table_name='recordings')
    op.drop_index('ix_recordings_channel_id', table_name='recordings')
    op.drop_table('recordings')

    op.drop_index('ix_recording_rules_title_match', table_name='recording_rules')
    op.drop_index('ix_recording_rules_channel_id', table_name='recording_rules')
    op.drop_table('recording_rules')
