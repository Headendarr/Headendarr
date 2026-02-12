"""add channel suggestions

Revision ID: 8a5b2d6e7f10
Revises: 7e3a4b9c2d1f
Create Date: 2026-02-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8a5b2d6e7f10'
down_revision = '7e3a4b9c2d1f'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'channel_suggestions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('channel_id', sa.Integer(), sa.ForeignKey('channels.id', ondelete='CASCADE'), nullable=False),
        sa.Column('playlist_id', sa.Integer(), sa.ForeignKey('playlists.id'), nullable=False),
        sa.Column('stream_id', sa.Integer(), nullable=False),
        sa.Column('stream_name', sa.String(length=500), nullable=True),
        sa.Column('stream_url', sa.Text(), nullable=True),
        sa.Column('group_title', sa.String(length=500), nullable=True),
        sa.Column('playlist_name', sa.String(length=500), nullable=True),
        sa.Column('source_type', sa.String(length=16), nullable=False, server_default='M3U'),
        sa.Column('score', sa.Float(), nullable=False, server_default='0'),
        sa.Column('dismissed', sa.Boolean(), nullable=False, server_default=sa.text('false')),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.UniqueConstraint('channel_id', 'playlist_id', 'stream_id', name='uq_channel_suggestion_stream'),
    )
    op.create_index('ix_channel_suggestions_channel_id', 'channel_suggestions', ['channel_id'])
    op.create_index('ix_channel_suggestions_playlist_id', 'channel_suggestions', ['playlist_id'])
    op.create_index('ix_channel_suggestions_stream_id', 'channel_suggestions', ['stream_id'])


def downgrade():
    op.drop_index('ix_channel_suggestions_stream_id', table_name='channel_suggestions')
    op.drop_index('ix_channel_suggestions_playlist_id', table_name='channel_suggestions')
    op.drop_index('ix_channel_suggestions_channel_id', table_name='channel_suggestions')
    op.drop_table('channel_suggestions')
