"""model explicit index and constraint definitions

Revision ID: c82c585b57eb
Revises: 96b9a47d7283
Create Date: 2026-04-27 17:28:30.026924

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c82c585b57eb"
down_revision = "96b9a47d7283"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def _has_index(table_name: str, index_name: str) -> bool:
    return any(index["name"] == index_name for index in _inspector().get_indexes(table_name))


def _has_unique_constraint(table_name: str, constraint_name: str) -> bool:
    return any(constraint["name"] == constraint_name for constraint in _inspector().get_unique_constraints(table_name))


def _column_info(table_name: str, column_name: str) -> dict[str, object] | None:
    for column in _inspector().get_columns(table_name):
        if column["name"] == column_name:
            return column
    return None


def _column_is_text(table_name: str, column_name: str) -> bool:
    column = _column_info(table_name, column_name)
    if column is None:
        return False
    return "TEXT" in str(column["type"]).upper()


def _fk_by_name(table_name: str, fk_name: str) -> dict[str, object] | None:
    for foreign_key in _inspector().get_foreign_keys(table_name):
        if foreign_key["name"] == fk_name:
            return foreign_key
    return None


def upgrade():
    if _has_unique_constraint("channel_tags", "channel_tags_name_key"):
        op.drop_constraint("channel_tags_name_key", "channel_tags", type_="unique")
    if not _has_index("channel_tags", "ix_channel_tags_name"):
        op.create_index("ix_channel_tags_name", "channel_tags", ["name"], unique=True)

    channel_suggestions_source_type = _column_info("channel_suggestions", "source_type")
    if channel_suggestions_source_type is not None and channel_suggestions_source_type.get("nullable") is False:
        op.alter_column(
            "channel_suggestions",
            "source_type",
            existing_type=sa.VARCHAR(length=16),
            nullable=True,
            existing_server_default=sa.text("'M3U'::character varying"),
        )

    for index_name, columns in (
        ("ix_channel_suggestions_group_title", ["group_title"]),
        ("ix_channel_suggestions_playlist_name", ["playlist_name"]),
        ("ix_channel_suggestions_source_type", ["source_type"]),
        ("ix_channel_suggestions_stream_name", ["stream_name"]),
    ):
        if not _has_index("channel_suggestions", index_name):
            op.create_index(index_name, "channel_suggestions", columns, unique=False)

    if _has_unique_constraint("channels", "uq_channels_number"):
        op.drop_constraint("uq_channels_number", "channels", type_="unique")

    if not _column_is_text("epgs", "url"):
        op.alter_column(
            "epgs",
            "url",
            existing_type=sa.VARCHAR(length=500),
            type_=sa.Text(),
            existing_nullable=True,
        )

    playlist_streams_playlist_id = _column_info("playlist_streams", "playlist_id")
    if playlist_streams_playlist_id is not None and playlist_streams_playlist_id.get("nullable") is False:
        op.alter_column(
            "playlist_streams",
            "playlist_id",
            existing_type=sa.INTEGER(),
            nullable=True,
        )

    for index_name, columns in (
        ("ix_playlist_streams_source_type", ["source_type"]),
        ("ix_playlist_streams_xc_category_id", ["xc_category_id"]),
        ("ix_playlist_streams_xc_stream_id", ["xc_stream_id"]),
    ):
        if not _has_index("playlist_streams", index_name):
            op.create_index(index_name, "playlist_streams", columns, unique=False)

    if _has_index("recording_rules", "ix_recording_rules_channel_id"):
        op.drop_index("ix_recording_rules_channel_id", table_name="recording_rules")
    if _has_index("recordings", "ix_recordings_channel_id"):
        op.drop_index("ix_recordings_channel_id", table_name="recordings")
    if not _has_index("recordings", "ix_recordings_title"):
        op.create_index("ix_recordings_title", "recordings", ["title"], unique=False)

    stream_audit_user_fk = _fk_by_name("stream_audit_logs", "stream_audit_logs_user_id_fkey")
    if stream_audit_user_fk is not None and stream_audit_user_fk.get("options", {}).get("ondelete") != "SET NULL":
        op.drop_constraint("stream_audit_logs_user_id_fkey", "stream_audit_logs", type_="foreignkey")
        stream_audit_user_fk = None
    if stream_audit_user_fk is None:
        op.create_foreign_key(
            "stream_audit_logs_user_id_fkey",
            "stream_audit_logs",
            "users",
            ["user_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    if _has_index("channel_tags", "ix_channel_tags_name"):
        op.drop_index("ix_channel_tags_name", table_name="channel_tags")
    if not _has_unique_constraint("channel_tags", "channel_tags_name_key"):
        op.create_unique_constraint("channel_tags_name_key", "channel_tags", ["name"])

    stream_audit_user_fk = _fk_by_name("stream_audit_logs", "stream_audit_logs_user_id_fkey")
    if stream_audit_user_fk is not None and stream_audit_user_fk.get("options", {}).get("ondelete") == "SET NULL":
        op.drop_constraint("stream_audit_logs_user_id_fkey", "stream_audit_logs", type_="foreignkey")
        op.create_foreign_key(
            "stream_audit_logs_user_id_fkey",
            "stream_audit_logs",
            "users",
            ["user_id"],
            ["id"],
        )

    if _has_index("recordings", "ix_recordings_title"):
        op.drop_index("ix_recordings_title", table_name="recordings")
    if not _has_index("recordings", "ix_recordings_channel_id"):
        op.create_index("ix_recordings_channel_id", "recordings", ["channel_id"], unique=False)
    if not _has_index("recording_rules", "ix_recording_rules_channel_id"):
        op.create_index("ix_recording_rules_channel_id", "recording_rules", ["channel_id"], unique=False)

    for index_name in (
        "ix_playlist_streams_xc_stream_id",
        "ix_playlist_streams_xc_category_id",
        "ix_playlist_streams_source_type",
    ):
        if _has_index("playlist_streams", index_name):
            op.drop_index(index_name, table_name="playlist_streams")

    playlist_streams_playlist_id = _column_info("playlist_streams", "playlist_id")
    if playlist_streams_playlist_id is not None and playlist_streams_playlist_id.get("nullable") is True:
        op.alter_column(
            "playlist_streams",
            "playlist_id",
            existing_type=sa.INTEGER(),
            nullable=False,
        )

    if _column_is_text("epgs", "url"):
        op.alter_column(
            "epgs",
            "url",
            existing_type=sa.Text(),
            type_=sa.VARCHAR(length=500),
            existing_nullable=True,
        )

    if not _has_unique_constraint("channels", "uq_channels_number"):
        op.create_unique_constraint(
            "uq_channels_number",
            "channels",
            ["number"],
            deferrable=True,
            initially="DEFERRED",
        )

    for index_name in (
        "ix_channel_suggestions_stream_name",
        "ix_channel_suggestions_source_type",
        "ix_channel_suggestions_playlist_name",
        "ix_channel_suggestions_group_title",
    ):
        if _has_index("channel_suggestions", index_name):
            op.drop_index(index_name, table_name="channel_suggestions")

    channel_suggestions_source_type = _column_info("channel_suggestions", "source_type")
    if channel_suggestions_source_type is not None and channel_suggestions_source_type.get("nullable") is True:
        op.alter_column(
            "channel_suggestions",
            "source_type",
            existing_type=sa.VARCHAR(length=16),
            nullable=False,
            existing_server_default=sa.text("'M3U'::character varying"),
        )
