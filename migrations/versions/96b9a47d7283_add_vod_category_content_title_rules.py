"""add_vod_category_content_title_rules

Revision ID: 96b9a47d7283
Revises: a6b7c8d9e0f1
Create Date: 2026-04-27 17:10:29.233278

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "96b9a47d7283"
down_revision = "a6b7c8d9e0f1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("vod_categories", sa.Column("content_title_rules", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("vod_categories", "content_title_rules")
