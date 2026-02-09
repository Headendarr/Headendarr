"""postgres text field expansions + merge heads

Revision ID: d7e8f9a0b1c2
Revises: 6c7a1f2d9b0e, 3a1f6d2a6a1f
Create Date: 2026-02-09 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd7e8f9a0b1c2'
down_revision = ('6c7a1f2d9b0e', '3a1f6d2a6a1f')
branch_labels = None
depends_on = None


def upgrade():
    # channels.logo_base64
    op.alter_column('channels', 'logo_base64', type_=sa.Text(), existing_type=sa.String(length=500), existing_nullable=True)
    op.alter_column('channels', 'logo_url', type_=sa.Text(), existing_type=sa.String(length=500), existing_nullable=True)

    # playlist_streams long URL fields
    op.alter_column('playlist_streams', 'url', type_=sa.Text(), existing_type=sa.String(length=500), existing_nullable=True)
    op.alter_column('playlist_streams', 'tvg_logo', type_=sa.Text(), existing_type=sa.String(length=500), existing_nullable=True)
    op.alter_column('playlist_streams', 'channel_id', type_=sa.Text(), existing_type=sa.String(length=500), existing_nullable=True)

    # playlists url
    op.alter_column('playlists', 'url', type_=sa.Text(), existing_type=sa.String(length=500), existing_nullable=True)

    # channel_sources long stream urls
    op.alter_column('channel_sources', 'playlist_stream_url', type_=sa.Text(), existing_type=sa.String(length=500), existing_nullable=True)

    # epg channels icon
    op.alter_column('epg_channels', 'icon_url', type_=sa.Text(), existing_type=sa.String(length=500), existing_nullable=True)

    # epg_channel_programmes text fields
    cols = [
        'channel_id', 'title', 'sub_title', 'desc', 'series_desc', 'country', 'icon_url',
        'start', 'stop', 'start_timestamp', 'stop_timestamp', 'categories', 'summary',
        'keywords', 'credits_json', 'video_colour', 'video_aspect', 'video_quality',
        'subtitles_type', 'previously_shown_date', 'epnum_onscreen', 'epnum_xmltv_ns',
        'epnum_dd_progid', 'star_rating', 'production_year', 'rating_system', 'rating_value',
    ]
    for col in cols:
        op.alter_column('epg_channel_programmes', col, type_=sa.Text())


def downgrade():
    # Downgrade to previous String lengths where known; leave as Text for safety.
    pass
