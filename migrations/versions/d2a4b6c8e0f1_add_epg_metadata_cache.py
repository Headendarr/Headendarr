"""add epg metadata cache

Revision ID: d2a4b6c8e0f1
Revises: c4d5e6f7a8b9
Create Date: 2026-03-10 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d2a4b6c8e0f1"
down_revision = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("epg_channel_programmes", sa.Column("metadata_lookup_hash", sa.String(length=64), nullable=True))
    op.create_index(
        "ix_epg_channel_programmes_metadata_lookup_hash",
        "epg_channel_programmes",
        ["metadata_lookup_hash"],
        unique=False,
    )

    op.create_table(
        "epg_programme_metadata_cache",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("lookup_hash", sa.String(length=64), nullable=False),
        sa.Column("lookup_title", sa.Text(), nullable=True),
        sa.Column("lookup_sub_title", sa.Text(), nullable=True),
        sa.Column("lookup_kind", sa.String(length=16), nullable=False),
        sa.Column("match_status", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("provider_item_type", sa.String(length=16), nullable=True),
        sa.Column("provider_item_id", sa.Integer(), nullable=True),
        sa.Column("provider_series_id", sa.Integer(), nullable=True),
        sa.Column("provider_season_number", sa.Integer(), nullable=True),
        sa.Column("provider_episode_number", sa.Integer(), nullable=True),
        sa.Column("cached_sub_title", sa.Text(), nullable=True),
        sa.Column("cached_desc", sa.Text(), nullable=True),
        sa.Column("cached_series_desc", sa.Text(), nullable=True),
        sa.Column("cached_icon_url", sa.Text(), nullable=True),
        sa.Column("cached_epnum_onscreen", sa.Text(), nullable=True),
        sa.Column("cached_epnum_xmltv_ns", sa.Text(), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("source_confidence", sa.Float(), nullable=True),
        sa.Column("raw_result_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lookup_hash"),
    )
    op.create_index(
        "ix_epg_programme_metadata_cache_expires_at",
        "epg_programme_metadata_cache",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_epg_programme_metadata_cache_lookup_hash",
        "epg_programme_metadata_cache",
        ["lookup_hash"],
        unique=False,
    )
    op.create_index(
        "ix_epg_programme_metadata_cache_match_status",
        "epg_programme_metadata_cache",
        ["match_status"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_epg_programme_metadata_cache_match_status", table_name="epg_programme_metadata_cache")
    op.drop_index("ix_epg_programme_metadata_cache_lookup_hash", table_name="epg_programme_metadata_cache")
    op.drop_index("ix_epg_programme_metadata_cache_expires_at", table_name="epg_programme_metadata_cache")
    op.drop_table("epg_programme_metadata_cache")
    op.drop_index("ix_epg_channel_programmes_metadata_lookup_hash", table_name="epg_channel_programmes")
    op.drop_column("epg_channel_programmes", "metadata_lookup_hash")
