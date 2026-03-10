"""add stream audit severity

Revision ID: c4d5e6f7a8b9
Revises: a9d4c2e8f1b7
Create Date: 2026-03-10 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c4d5e6f7a8b9"
down_revision = "a9d4c2e8f1b7"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "stream_audit_logs",
        sa.Column("severity", sa.String(length=16), nullable=True, server_default="info"),
    )
    op.execute("UPDATE stream_audit_logs SET severity = 'info' WHERE severity IS NULL")
    op.alter_column("stream_audit_logs", "severity", nullable=False, server_default="info")


def downgrade():
    op.drop_column("stream_audit_logs", "severity")
