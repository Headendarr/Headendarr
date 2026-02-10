"""add xc accounts table and channel source link

Revision ID: 9c2d3a4b5f6a
Revises: 5b2c8a1d7f3c
Create Date: 2026-02-10 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9c2d3a4b5f6a"
down_revision = "5b2c8a1d7f3c"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "xc_accounts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("playlist_id", sa.Integer(), sa.ForeignKey("playlists.id"), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=False),
        sa.Column("password", sa.String(length=255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("connection_limit", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("label", sa.String(length=255), nullable=True),
        sa.Column("tvh_uuid", sa.String(length=64), nullable=True),
    )
    op.create_index("ix_xc_accounts_tvh_uuid", "xc_accounts", ["tvh_uuid"], unique=True)

    with op.batch_alter_table("channel_sources") as batch_op:
        batch_op.add_column(sa.Column("xc_account_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_channel_sources_xc_account_id",
            "xc_accounts",
            ["xc_account_id"],
            ["id"],
        )

    # Migrate existing XC credentials into xc_accounts
    conn = op.get_bind()
    playlists = conn.execute(
        sa.text(
            "SELECT id, xc_username, xc_password FROM playlists "
            "WHERE account_type = 'XC' AND xc_username IS NOT NULL AND xc_password IS NOT NULL"
        )
    ).fetchall()
    for row in playlists:
        conn.execute(
            sa.text(
                "INSERT INTO xc_accounts (playlist_id, username, password, enabled, connection_limit) "
                "VALUES (:playlist_id, :username, :password, :enabled, :connection_limit)"
            ),
            {
                "playlist_id": row[0],
                "username": row[1],
                "password": row[2],
                "enabled": True,
                "connection_limit": 1,
            },
        )


def downgrade():
    with op.batch_alter_table("channel_sources") as batch_op:
        batch_op.drop_constraint("fk_channel_sources_xc_account_id", type_="foreignkey")
        batch_op.drop_column("xc_account_id")

    op.drop_index("ix_xc_accounts_tvh_uuid", table_name="xc_accounts")
    op.drop_table("xc_accounts")
