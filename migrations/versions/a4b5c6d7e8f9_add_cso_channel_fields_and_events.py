"""add cso channel fields and events

Revision ID: a4b5c6d7e8f9
Revises: f2c4d6e8a9b1
Create Date: 2026-02-20 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a4b5c6d7e8f9"
down_revision = "f2c4d6e8a9b1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "channels",
        sa.Column("cso_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "channels",
        sa.Column("cso_policy", sa.Text(), nullable=True),
    )

    op.create_table(
        "cso_event_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("channel_id", sa.Integer(), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column("playlist_id", sa.Integer(), nullable=True),
        sa.Column("recording_id", sa.Integer(), nullable=True),
        sa.Column("tvh_subscription_id", sa.String(length=128), nullable=True),
        sa.Column("session_id", sa.String(length=128), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False, server_default="info"),
        sa.Column("details_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["channel_id"], ["channels.id"]),
        sa.ForeignKeyConstraint(["source_id"], ["channel_sources.id"]),
        sa.ForeignKeyConstraint(["playlist_id"], ["playlists.id"]),
        sa.ForeignKeyConstraint(["recording_id"], ["recordings.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_cso_event_logs_channel_created",
        "cso_event_logs",
        ["channel_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_cso_event_logs_event_type_created",
        "cso_event_logs",
        ["event_type", "created_at"],
        unique=False,
    )
    op.create_index("ix_cso_event_logs_channel_id", "cso_event_logs", ["channel_id"], unique=False)
    op.create_index("ix_cso_event_logs_source_id", "cso_event_logs", ["source_id"], unique=False)
    op.create_index("ix_cso_event_logs_playlist_id", "cso_event_logs", ["playlist_id"], unique=False)
    op.create_index("ix_cso_event_logs_recording_id", "cso_event_logs", ["recording_id"], unique=False)
    op.create_index(
        "ix_cso_event_logs_tvh_subscription_id",
        "cso_event_logs",
        ["tvh_subscription_id"],
        unique=False,
    )
    op.create_index("ix_cso_event_logs_session_id", "cso_event_logs", ["session_id"], unique=False)
    op.create_index("ix_cso_event_logs_event_type", "cso_event_logs", ["event_type"], unique=False)

    # remove transient server defaults
    op.alter_column("channels", "cso_enabled", server_default=None)
    op.alter_column("cso_event_logs", "severity", server_default=None)


def downgrade():
    op.drop_index("ix_cso_event_logs_event_type", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_session_id", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_tvh_subscription_id", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_recording_id", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_playlist_id", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_source_id", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_channel_id", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_event_type_created", table_name="cso_event_logs")
    op.drop_index("ix_cso_event_logs_channel_created", table_name="cso_event_logs")
    op.drop_table("cso_event_logs")

    op.drop_column("channels", "cso_policy")
    op.drop_column("channels", "cso_enabled")
