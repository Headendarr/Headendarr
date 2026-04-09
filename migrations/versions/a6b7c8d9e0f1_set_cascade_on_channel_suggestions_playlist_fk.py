"""set cascade on channel suggestions playlist foreign key

Revision ID: a6b7c8d9e0f1
Revises: e5ffa1b2c3d4
Create Date: 2026-04-10 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a6b7c8d9e0f1"
down_revision = "e5ffa1b2c3d4"
branch_labels = None
depends_on = None


def _drop_playlist_fk(batch_op, bind):
    inspector = sa.inspect(bind)
    for fk in inspector.get_foreign_keys("channel_suggestions"):
        fk_columns = fk.get("constrained_columns") or []
        fk_name = fk.get("name")
        if fk_columns == ["playlist_id"] and fk.get("referred_table") == "playlists" and fk_name:
            batch_op.drop_constraint(fk_name, type_="foreignkey")
            return


def upgrade():
    bind = op.get_bind()
    with op.batch_alter_table("channel_suggestions") as batch_op:
        _drop_playlist_fk(batch_op, bind)
        batch_op.create_foreign_key(
            "fk_channel_suggestions_playlist_id_playlists",
            "playlists",
            ["playlist_id"],
            ["id"],
            ondelete="CASCADE",
        )


def downgrade():
    with op.batch_alter_table("channel_suggestions") as batch_op:
        batch_op.drop_constraint("fk_channel_suggestions_playlist_id_playlists", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_channel_suggestions_playlist_id_playlists",
            "playlists",
            ["playlist_id"],
            ["id"],
        )
