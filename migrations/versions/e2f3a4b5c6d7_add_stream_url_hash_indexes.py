"""add stream url hash indexes

Revision ID: e2f3a4b5c6d7
Revises: d1e2f3a4b5c6
Create Date: 2026-03-07 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
import hashlib


# revision identifiers, used by Alembic.
revision = "e2f3a4b5c6d7"
down_revision = "d1e2f3a4b5c6"
branch_labels = None
depends_on = None


def _fast_url_hash(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return hashlib.md5(raw.encode("utf-8"), usedforsecurity=False).hexdigest()


def _index_names(bind, table_name):
    inspector = sa.inspect(bind)
    return {idx.get("name") for idx in inspector.get_indexes(table_name) if idx.get("name")}


def upgrade():
    bind = op.get_bind()
    existing_indexes = _index_names(bind, "playlist_streams")

    with op.batch_alter_table("playlist_streams") as batch_op:
        batch_op.add_column(sa.Column("url_hash", sa.String(length=32), nullable=True))
        if "ix_playlist_streams_url" in existing_indexes:
            batch_op.drop_index("ix_playlist_streams_url")
        if "ix_playlist_streams_playlist_id" not in existing_indexes:
            batch_op.create_index("ix_playlist_streams_playlist_id", ["playlist_id"], unique=False)
        if "ix_playlist_streams_playlist_id_url_hash" not in existing_indexes:
            batch_op.create_index(
                "ix_playlist_streams_playlist_id_url_hash",
                ["playlist_id", "url_hash"],
                unique=False,
            )

    playlist_streams = sa.table(
        "playlist_streams",
        sa.column("id", sa.Integer),
        sa.column("url", sa.Text),
        sa.column("url_hash", sa.String(length=32)),
    )
    rows = bind.execute(
        sa.select(playlist_streams.c.id, playlist_streams.c.url).where(playlist_streams.c.url.is_not(None))
    ).all()
    updates = []
    for row in rows:
        updates.append({"row_id": row.id, "new_hash": _fast_url_hash(row.url)})
    if updates:
        bind.execute(
            playlist_streams.update()
            .where(playlist_streams.c.id == sa.bindparam("row_id"))
            .values(url_hash=sa.bindparam("new_hash")),
            updates,
        )

    channel_source_indexes = _index_names(bind, "channel_sources")
    with op.batch_alter_table("channel_sources") as batch_op:
        if "ix_channel_sources_playlist_stream_url" in channel_source_indexes:
            batch_op.drop_index("ix_channel_sources_playlist_stream_url")

    playlist_indexes = _index_names(bind, "playlists")
    with op.batch_alter_table("playlists") as batch_op:
        if "ix_playlists_url" in playlist_indexes:
            batch_op.drop_index("ix_playlists_url")

    epg_indexes = _index_names(bind, "epgs")
    with op.batch_alter_table("epgs") as batch_op:
        if "ix_epgs_url" in epg_indexes:
            batch_op.drop_index("ix_epgs_url")


def downgrade():
    bind = op.get_bind()
    existing_indexes = _index_names(bind, "playlist_streams")

    with op.batch_alter_table("playlist_streams") as batch_op:
        if "ix_playlist_streams_playlist_id_url_hash" in existing_indexes:
            batch_op.drop_index("ix_playlist_streams_playlist_id_url_hash")
        if "ix_playlist_streams_playlist_id" in existing_indexes:
            batch_op.drop_index("ix_playlist_streams_playlist_id")
        if "ix_playlist_streams_url" not in existing_indexes:
            batch_op.create_index("ix_playlist_streams_url", ["url"], unique=False)
        batch_op.drop_column("url_hash")

    channel_source_indexes = _index_names(bind, "channel_sources")
    with op.batch_alter_table("channel_sources") as batch_op:
        if "ix_channel_sources_playlist_stream_url" not in channel_source_indexes:
            batch_op.create_index("ix_channel_sources_playlist_stream_url", ["playlist_stream_url"], unique=False)

    playlist_indexes = _index_names(bind, "playlists")
    with op.batch_alter_table("playlists") as batch_op:
        if "ix_playlists_url" not in playlist_indexes:
            batch_op.create_index("ix_playlists_url", ["url"], unique=False)

    epg_indexes = _index_names(bind, "epgs")
    with op.batch_alter_table("epgs") as batch_op:
        if "ix_epgs_url" not in epg_indexes:
            batch_op.create_index("ix_epgs_url", ["url"], unique=False)
