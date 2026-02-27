"""add epg update schedule

Revision ID: e1b2c3d4e5f6
Revises: a4b5c6d7e8f9
Create Date: 2026-02-27 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e1b2c3d4e5f6"
down_revision = "a4b5c6d7e8f9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "epgs",
        sa.Column("update_schedule", sa.String(length=16), nullable=False, server_default="12h"),
    )
    op.alter_column("epgs", "update_schedule", server_default=None)


def downgrade():
    op.drop_column("epgs", "update_schedule")
