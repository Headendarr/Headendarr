"""stream audit log text fields

Revision ID: 5b2c8a1d7f3c
Revises: d7e8f9a0b1c2
Create Date: 2026-02-10 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5b2c8a1d7f3c'
down_revision = 'd7e8f9a0b1c2'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        'stream_audit_logs',
        'endpoint',
        type_=sa.Text(),
        existing_type=sa.String(length=255),
        existing_nullable=True,
    )
    op.alter_column(
        'stream_audit_logs',
        'user_agent',
        type_=sa.Text(),
        existing_type=sa.String(length=255),
        existing_nullable=True,
    )
    op.alter_column(
        'stream_audit_logs',
        'details',
        type_=sa.Text(),
        existing_type=sa.String(length=1024),
        existing_nullable=True,
    )


def downgrade():
    # Downgrade not supported; keep Text to avoid truncation.
    pass
