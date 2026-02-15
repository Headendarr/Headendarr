"""add recording owner/profile fields

Revision ID: c3f9a1b8d4e2
Revises: b1f4d2e8a6c1
Create Date: 2026-02-15 16:10:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c3f9a1b8d4e2'
down_revision = 'b1f4d2e8a6c1'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('recording_rules', sa.Column('owner_user_id', sa.Integer(), nullable=True))
    op.add_column('recording_rules', sa.Column('recording_profile_key', sa.String(length=64), nullable=False, server_default='default'))
    op.create_index('ix_recording_rules_owner_user_id', 'recording_rules', ['owner_user_id'])
    op.create_foreign_key(
        'fk_recording_rules_owner_user_id_users',
        'recording_rules',
        'users',
        ['owner_user_id'],
        ['id'],
    )

    op.add_column('recordings', sa.Column('owner_user_id', sa.Integer(), nullable=True))
    op.add_column('recordings', sa.Column('recording_profile_key', sa.String(length=64), nullable=False, server_default='default'))
    op.create_index('ix_recordings_owner_user_id', 'recordings', ['owner_user_id'])
    op.create_foreign_key(
        'fk_recordings_owner_user_id_users',
        'recordings',
        'users',
        ['owner_user_id'],
        ['id'],
    )

    op.alter_column('recording_rules', 'recording_profile_key', server_default=None)
    op.alter_column('recordings', 'recording_profile_key', server_default=None)

    op.add_column('users', sa.Column('dvr_access_mode', sa.String(length=32), nullable=False, server_default='none'))
    op.add_column('users', sa.Column('dvr_retention_policy', sa.String(length=32), nullable=False, server_default='forever'))

    op.alter_column('users', 'dvr_access_mode', server_default=None)
    op.alter_column('users', 'dvr_retention_policy', server_default=None)


def downgrade():
    op.drop_column('users', 'dvr_retention_policy')
    op.drop_column('users', 'dvr_access_mode')

    op.drop_constraint('fk_recordings_owner_user_id_users', 'recordings', type_='foreignkey')
    op.drop_index('ix_recordings_owner_user_id', table_name='recordings')
    op.drop_column('recordings', 'recording_profile_key')
    op.drop_column('recordings', 'owner_user_id')

    op.drop_constraint('fk_recording_rules_owner_user_id_users', 'recording_rules', type_='foreignkey')
    op.drop_index('ix_recording_rules_owner_user_id', table_name='recording_rules')
    op.drop_column('recording_rules', 'recording_profile_key')
    op.drop_column('recording_rules', 'owner_user_id')
