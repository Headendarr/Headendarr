"""add user last stream key used timestamp

Revision ID: b8c9d0e1f2a3
Revises: a7b8c9d0e1f2
Create Date: 2026-02-27 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b8c9d0e1f2a3"
down_revision = "a7b8c9d0e1f2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("users", sa.Column("last_stream_key_used_at", sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column("users", "last_stream_key_used_at")
