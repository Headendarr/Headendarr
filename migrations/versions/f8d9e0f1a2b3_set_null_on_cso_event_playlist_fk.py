"""set null on cso event playlist foreign key

Revision ID: f8d9e0f1a2b3
Revises: c6d7e8f9a0b1
Create Date: 2026-03-18 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f8d9e0f1a2b3"
down_revision = "c6d7e8f9a0b1"
branch_labels = None
depends_on = None


def _drop_playlist_fk(batch_op, bind):
    inspector = sa.inspect(bind)
    for fk in inspector.get_foreign_keys("cso_event_logs"):
        fk_columns = fk.get("constrained_columns") or []
        fk_name = fk.get("name")
        if fk_columns == ["playlist_id"] and fk.get("referred_table") == "playlists" and fk_name:
            batch_op.drop_constraint(fk_name, type_="foreignkey")
            return


def upgrade():
    bind = op.get_bind()
    with op.batch_alter_table("cso_event_logs") as batch_op:
        _drop_playlist_fk(batch_op, bind)
        batch_op.create_foreign_key(
            "fk_cso_event_logs_playlist_id_playlists",
            "playlists",
            ["playlist_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    with op.batch_alter_table("cso_event_logs") as batch_op:
        batch_op.drop_constraint("fk_cso_event_logs_playlist_id_playlists", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_cso_event_logs_playlist_id_playlists",
            "playlists",
            ["playlist_id"],
            ["id"],
        )
