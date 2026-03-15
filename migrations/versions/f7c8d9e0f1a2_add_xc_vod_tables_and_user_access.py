"""add xc vod tables and user access

Revision ID: f7c8d9e0f1a2
Revises: f6b7c8d9e0f1
Create Date: 2026-03-14 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f7c8d9e0f1a2"
down_revision = "f6b7c8d9e0f1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "users",
        sa.Column("vod_access_mode", sa.String(length=32), nullable=False, server_default="none"),
    )
    op.add_column(
        "users",
        sa.Column("vod_generate_strm_files", sa.Boolean(), nullable=False, server_default=sa.false()),
    )

    op.create_table(
        "xc_vod_metadata_cache",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("playlist_id", sa.Integer(), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("upstream_item_id", sa.String(length=128), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("last_requested_at", sa.DateTime(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["playlist_id"], ["playlists.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("playlist_id", "action", "upstream_item_id", name="uq_xc_vod_metadata_cache_lookup"),
    )
    op.create_index("ix_xc_vod_metadata_cache_playlist_id", "xc_vod_metadata_cache", ["playlist_id"])
    op.create_index("ix_xc_vod_metadata_cache_action", "xc_vod_metadata_cache", ["action"])
    op.create_index("ix_xc_vod_metadata_cache_upstream_item_id", "xc_vod_metadata_cache", ["upstream_item_id"])
    op.create_index("ix_xc_vod_metadata_cache_expires_at", "xc_vod_metadata_cache", ["expires_at"])
    op.create_index("ix_xc_vod_metadata_cache_last_requested_at", "xc_vod_metadata_cache", ["last_requested_at"])

    op.create_table(
        "xc_vod_categories",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("playlist_id", sa.Integer(), nullable=False),
        sa.Column("category_type", sa.String(length=16), nullable=False),
        sa.Column("upstream_category_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=500), nullable=False),
        sa.Column("parent_id", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(["playlist_id"], ["playlists.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("playlist_id", "category_type", "upstream_category_id", name="uq_xc_vod_category_upstream"),
    )
    op.create_index("ix_xc_vod_categories_playlist_id", "xc_vod_categories", ["playlist_id"])
    op.create_index("ix_xc_vod_categories_category_type", "xc_vod_categories", ["category_type"])
    op.create_index("ix_xc_vod_categories_upstream_category_id", "xc_vod_categories", ["upstream_category_id"])
    op.create_index("ix_xc_vod_categories_name", "xc_vod_categories", ["name"])

    op.create_table(
        "xc_vod_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("playlist_id", sa.Integer(), nullable=False),
        sa.Column("category_id", sa.Integer(), nullable=True),
        sa.Column("item_type", sa.String(length=16), nullable=False),
        sa.Column("upstream_item_id", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("sort_title", sa.String(length=500), nullable=True),
        sa.Column("release_date", sa.String(length=64), nullable=True),
        sa.Column("year", sa.String(length=16), nullable=True),
        sa.Column("rating", sa.String(length=64), nullable=True),
        sa.Column("poster_url", sa.Text(), nullable=True),
        sa.Column("container_extension", sa.String(length=32), nullable=True),
        sa.Column("direct_source", sa.Text(), nullable=True),
        sa.Column("added", sa.String(length=64), nullable=True),
        sa.Column("summary_json", sa.Text(), nullable=True),
        sa.Column("stream_probe_details", sa.Text(), nullable=True),
        sa.Column("stream_probe_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["playlist_id"], ["playlists.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["category_id"], ["xc_vod_categories.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("playlist_id", "item_type", "upstream_item_id", name="uq_xc_vod_item_upstream"),
    )
    op.create_index("ix_xc_vod_items_playlist_id", "xc_vod_items", ["playlist_id"])
    op.create_index("ix_xc_vod_items_category_id", "xc_vod_items", ["category_id"])
    op.create_index("ix_xc_vod_items_item_type", "xc_vod_items", ["item_type"])
    op.create_index("ix_xc_vod_items_upstream_item_id", "xc_vod_items", ["upstream_item_id"])
    op.create_index("ix_xc_vod_items_title", "xc_vod_items", ["title"])
    op.create_index("ix_xc_vod_items_sort_title", "xc_vod_items", ["sort_title"])

    op.create_table(
        "vod_categories",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("content_type", sa.String(length=16), nullable=False),
        sa.Column("name", sa.String(length=500), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("profile_id", sa.String(length=64), nullable=True),
        sa.Column("generate_strm_files", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("strm_base_url", sa.String(length=1024), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("content_type", "name", name="uq_vod_category_content_type_name"),
    )
    op.create_index("ix_vod_categories_content_type", "vod_categories", ["content_type"])
    op.create_index("ix_vod_categories_name", "vod_categories", ["name"])

    op.create_table(
        "vod_category_xc_categories",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("category_id", sa.Integer(), nullable=False),
        sa.Column("xc_category_id", sa.Integer(), nullable=False),
        sa.Column("strip_title_prefixes", sa.Text(), nullable=True),
        sa.Column("strip_title_suffixes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["category_id"], ["vod_categories.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["xc_category_id"], ["xc_vod_categories.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("category_id", "xc_category_id", name="uq_vod_category_xc_category"),
    )
    op.create_index("ix_vod_category_xc_categories_category_id", "vod_category_xc_categories", ["category_id"])
    op.create_index("ix_vod_category_xc_categories_xc_category_id", "vod_category_xc_categories", ["xc_category_id"])

    op.create_table(
        "vod_category_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("category_id", sa.Integer(), nullable=False),
        sa.Column("item_type", sa.String(length=16), nullable=False),
        sa.Column("dedupe_key", sa.String(length=512), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("sort_title", sa.String(length=500), nullable=True),
        sa.Column("release_date", sa.String(length=64), nullable=True),
        sa.Column("year", sa.String(length=16), nullable=True),
        sa.Column("rating", sa.String(length=64), nullable=True),
        sa.Column("poster_url", sa.Text(), nullable=True),
        sa.Column("container_extension", sa.String(length=32), nullable=True),
        sa.Column("summary_json", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["category_id"], ["vod_categories.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("category_id", "dedupe_key", name="uq_vod_category_item_dedupe"),
    )
    op.create_index("ix_vod_category_items_category_id", "vod_category_items", ["category_id"])
    op.create_index("ix_vod_category_items_item_type", "vod_category_items", ["item_type"])
    op.create_index("ix_vod_category_items_dedupe_key", "vod_category_items", ["dedupe_key"])
    op.create_index("ix_vod_category_items_title", "vod_category_items", ["title"])
    op.create_index("ix_vod_category_items_sort_title", "vod_category_items", ["sort_title"])

    op.create_table(
        "vod_category_item_sources",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("category_item_id", sa.Integer(), nullable=False),
        sa.Column("source_item_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["category_item_id"], ["vod_category_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_item_id"], ["xc_vod_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("category_item_id", "source_item_id", name="uq_vod_category_item_source"),
    )
    op.create_index("ix_vod_category_item_sources_category_item_id", "vod_category_item_sources", ["category_item_id"])
    op.create_index("ix_vod_category_item_sources_source_item_id", "vod_category_item_sources", ["source_item_id"])

    op.create_table(
        "vod_category_episodes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("category_item_id", sa.Integer(), nullable=False),
        sa.Column("dedupe_key", sa.String(length=512), nullable=False),
        sa.Column("season_number", sa.Integer(), nullable=True),
        sa.Column("episode_number", sa.Integer(), nullable=True),
        sa.Column("title", sa.String(length=500), nullable=True),
        sa.Column("container_extension", sa.String(length=32), nullable=True),
        sa.Column("summary_json", sa.Text(), nullable=True),
        sa.Column("stream_probe_details", sa.Text(), nullable=True),
        sa.Column("stream_probe_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["category_item_id"], ["vod_category_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("category_item_id", "dedupe_key", name="uq_vod_category_episode_dedupe"),
    )
    op.create_index("ix_vod_category_episodes_category_item_id", "vod_category_episodes", ["category_item_id"])
    op.create_index("ix_vod_category_episodes_dedupe_key", "vod_category_episodes", ["dedupe_key"])

    op.create_table(
        "vod_category_episode_sources",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("episode_id", sa.Integer(), nullable=False),
        sa.Column("category_item_source_id", sa.Integer(), nullable=False),
        sa.Column("upstream_episode_id", sa.String(length=128), nullable=False),
        sa.Column("season_number", sa.Integer(), nullable=True),
        sa.Column("episode_number", sa.Integer(), nullable=True),
        sa.Column("title", sa.String(length=500), nullable=True),
        sa.Column("container_extension", sa.String(length=32), nullable=True),
        sa.Column("summary_json", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["episode_id"], ["vod_category_episodes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["category_item_source_id"], ["vod_category_item_sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("episode_id", "category_item_source_id", "upstream_episode_id", name="uq_vod_category_episode_source"),
    )
    op.create_index("ix_vod_category_episode_sources_episode_id", "vod_category_episode_sources", ["episode_id"])
    op.create_index("ix_vod_category_episode_sources_category_item_source_id", "vod_category_episode_sources", ["category_item_source_id"])
    op.create_index("ix_vod_category_episode_sources_upstream_episode_id", "vod_category_episode_sources", ["upstream_episode_id"])

    # CSO Event Logs expansion
    op.add_column("cso_event_logs", sa.Column("vod_category_id", sa.Integer(), nullable=True))
    op.add_column("cso_event_logs", sa.Column("vod_item_id", sa.Integer(), nullable=True))
    op.add_column("cso_event_logs", sa.Column("vod_episode_id", sa.Integer(), nullable=True))
    op.create_foreign_key("fk_cso_event_logs_vod_category_id", "cso_event_logs", "vod_categories", ["vod_category_id"], ["id"], ondelete="SET NULL")
    op.create_foreign_key("fk_cso_event_logs_vod_item_id", "cso_event_logs", "vod_category_items", ["vod_item_id"], ["id"], ondelete="SET NULL")
    op.create_foreign_key("fk_cso_event_logs_vod_episode_id", "cso_event_logs", "vod_category_episodes", ["vod_episode_id"], ["id"], ondelete="SET NULL")
    op.create_index("ix_cso_event_logs_vod_category_id", "cso_event_logs", ["vod_category_id"])
    op.create_index("ix_cso_event_logs_vod_item_id", "cso_event_logs", ["vod_item_id"])
    op.create_index("ix_cso_event_logs_vod_episode_id", "cso_event_logs", ["vod_episode_id"])


def downgrade():
    op.drop_index("ix_cso_event_logs_vod_episode_id", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_vod_item_id", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_vod_category_id", table_name="cso_event_logs")
    op.drop_constraint("fk_cso_event_logs_vod_episode_id", "cso_event_logs", type_="foreignkey")
    op.drop_constraint("fk_cso_event_logs_vod_item_id", "cso_event_logs", type_="foreignkey")
    op.drop_constraint("fk_cso_event_logs_vod_category_id", "cso_event_logs", type_="foreignkey")
    op.drop_column("cso_event_logs", "vod_episode_id")
    op.drop_column("cso_event_logs", "vod_item_id")
    op.drop_column("cso_event_logs", "vod_category_id")

    op.drop_table("vod_category_episode_sources")
    op.drop_table("vod_category_episodes")
    op.drop_table("vod_category_item_sources")
    op.drop_table("vod_category_items")
    op.drop_table("vod_category_xc_categories")
    op.drop_table("vod_categories")
    op.drop_table("xc_vod_metadata_cache")
    op.drop_table("xc_vod_items")
    op.drop_table("xc_vod_categories")

    op.drop_column("users", "vod_generate_strm_files")
    op.drop_column("users", "vod_access_mode")
