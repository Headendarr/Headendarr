"""add oidc fields to users

Revision ID: a1b2c3d4e5f7
Revises: f4a5b6c7d8e9
Create Date: 2026-03-07 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f7"
down_revision = "f4a5b6c7d8e9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("users", sa.Column("auth_source", sa.String(length=32), nullable=False, server_default="local"))
    op.add_column("users", sa.Column("oidc_issuer", sa.String(length=255), nullable=True))
    op.add_column("users", sa.Column("oidc_subject", sa.String(length=255), nullable=True))
    op.add_column("users", sa.Column("oidc_email", sa.String(length=255), nullable=True))
    op.create_index(
        "ix_users_oidc_issuer_subject",
        "users",
        ["oidc_issuer", "oidc_subject"],
        unique=True,
    )


def downgrade():
    op.drop_index("ix_users_oidc_issuer_subject", table_name="users")
    op.drop_column("users", "oidc_email")
    op.drop_column("users", "oidc_subject")
    op.drop_column("users", "oidc_issuer")
    op.drop_column("users", "auth_source")
