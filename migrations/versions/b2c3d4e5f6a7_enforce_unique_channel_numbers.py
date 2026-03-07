"""enforce unique channel numbers

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f7
Create Date: 2026-03-07 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f7"
branch_labels = None
depends_on = None


def _dedupe_channel_numbers(bind):
    rows = bind.execute(
        sa.text(
            """
            SELECT id, number
            FROM channels
            WHERE number IS NOT NULL
            ORDER BY number ASC, id ASC
            """
        )
    ).fetchall()

    last_number = None
    for row in rows:
        channel_id = int(row.id)
        current_number = int(row.number)
        if last_number is None or current_number > last_number:
            target_number = current_number
        else:
            target_number = last_number + 1
        if target_number != current_number:
            bind.execute(
                sa.text(
                    """
                    UPDATE channels
                    SET number = :number
                    WHERE id = :channel_id
                    """
                ),
                {"number": target_number, "channel_id": channel_id},
            )
        last_number = target_number


def upgrade():
    bind = op.get_bind()
    _dedupe_channel_numbers(bind)
    op.create_unique_constraint(
        "uq_channels_number",
        "channels",
        ["number"],
        deferrable=True,
        initially="DEFERRED",
    )


def downgrade():
    op.drop_constraint("uq_channels_number", "channels", type_="unique")
