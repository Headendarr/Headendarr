"""set null on cso event channel and recording foreign keys

Revision ID: f6b7c8d9e0f1
Revises: f5a6b7c8d9e0
Create Date: 2026-03-13 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f6b7c8d9e0f1"
down_revision = "f5a6b7c8d9e0"
branch_labels = None
depends_on = None


def _drop_fk(batch_op, bind, constrained_columns, referred_table):
    inspector = sa.inspect(bind)
    for fk in inspector.get_foreign_keys("cso_event_logs"):
        fk_columns = fk.get("constrained_columns") or []
        fk_name = fk.get("name")
        if fk_columns == constrained_columns and fk.get("referred_table") == referred_table and fk_name:
            batch_op.drop_constraint(fk_name, type_="foreignkey")
            return


def upgrade():
    bind = op.get_bind()
    with op.batch_alter_table("cso_event_logs") as batch_op:
        _drop_fk(batch_op, bind, ["channel_id"], "channels")
        _drop_fk(batch_op, bind, ["recording_id"], "recordings")
        batch_op.create_foreign_key(
            "fk_cso_event_logs_channel_id_channels",
            "channels",
            ["channel_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.create_foreign_key(
            "fk_cso_event_logs_recording_id_recordings",
            "recordings",
            ["recording_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    with op.batch_alter_table("cso_event_logs") as batch_op:
        batch_op.drop_constraint("fk_cso_event_logs_channel_id_channels", type_="foreignkey")
        batch_op.drop_constraint("fk_cso_event_logs_recording_id_recordings", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_cso_event_logs_channel_id_channels",
            "channels",
            ["channel_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_cso_event_logs_recording_id_recordings",
            "recordings",
            ["recording_id"],
            ["id"],
        )
