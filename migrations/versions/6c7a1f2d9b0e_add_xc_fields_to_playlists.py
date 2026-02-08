"""add xc fields to playlists

Revision ID: 6c7a1f2d9b0e
Revises: 2b7d9c4e1a2f
Create Date: 2026-02-08 20:13:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6c7a1f2d9b0e"
down_revision = "2b7d9c4e1a2f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("playlists") as batch_op:
        batch_op.add_column(sa.Column("account_type", sa.String(length=16), nullable=False, server_default="M3U"))
        batch_op.add_column(sa.Column("xc_username", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("xc_password", sa.String(length=255), nullable=True))

    with op.batch_alter_table("playlist_streams") as batch_op:
        batch_op.add_column(sa.Column("source_type", sa.String(length=16), nullable=True, server_default="M3U"))
        batch_op.add_column(sa.Column("xc_stream_id", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("xc_category_id", sa.Integer(), nullable=True))


def downgrade():
    with op.batch_alter_table("playlist_streams") as batch_op:
        batch_op.drop_column("xc_category_id")
        batch_op.drop_column("xc_stream_id")
        batch_op.drop_column("source_type")

    with op.batch_alter_table("playlists") as batch_op:
        batch_op.drop_column("xc_password")
        batch_op.drop_column("xc_username")
        batch_op.drop_column("account_type")
