"""add channel guide offset minutes

Revision ID: a9d4c2e8f1b7
Revises: b2c3d4e5f6a7
Create Date: 2026-03-09 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a9d4c2e8f1b7"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("channels", sa.Column("guide_offset_minutes", sa.Integer(), nullable=False, server_default="0"))
    op.alter_column("channels", "guide_offset_minutes", server_default=None)


def downgrade():
    op.drop_column("channels", "guide_offset_minutes")
