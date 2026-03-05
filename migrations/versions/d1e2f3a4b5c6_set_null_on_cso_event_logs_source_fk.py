"""set null on cso event logs source foreign key

Revision ID: d1e2f3a4b5c6
Revises: c9d8e7f6a5b4
Create Date: 2026-03-06 08:40:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d1e2f3a4b5c6"
down_revision = "c9d8e7f6a5b4"
branch_labels = None
depends_on = None


def _drop_source_fk(batch_op, bind):
    inspector = sa.inspect(bind)
    for fk in inspector.get_foreign_keys("cso_event_logs"):
        constrained_columns = fk.get("constrained_columns") or []
        referred_table = fk.get("referred_table")
        fk_name = fk.get("name")
        if constrained_columns == ["source_id"] and referred_table == "channel_sources" and fk_name:
            batch_op.drop_constraint(fk_name, type_="foreignkey")
            return


def upgrade():
    bind = op.get_bind()
    with op.batch_alter_table("cso_event_logs") as batch_op:
        _drop_source_fk(batch_op, bind)
        batch_op.create_foreign_key(
            "fk_cso_event_logs_source_id_channel_sources",
            "channel_sources",
            ["source_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    with op.batch_alter_table("cso_event_logs") as batch_op:
        batch_op.drop_constraint("fk_cso_event_logs_source_id_channel_sources", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_cso_event_logs_source_id_channel_sources",
            "channel_sources",
            ["source_id"],
            ["id"],
        )
