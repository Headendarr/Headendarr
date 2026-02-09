"""user management tables

Revision ID: 5c1b2e8b7e0a
Revises: f3d254922d25
Create Date: 2026-02-07 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5c1b2e8b7e0a'
down_revision = 'f3d254922d25'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'roles',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=32), nullable=False),
        sa.Column('description', sa.String(length=255), nullable=True),
    )
    op.create_index('ix_roles_name', 'roles', ['name'], unique=True)

    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('username', sa.String(length=64), nullable=False),
        sa.Column('password_hash', sa.String(length=255), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('streaming_key', sa.String(length=255), nullable=True),
        sa.Column('streaming_key_hash', sa.String(length=255), nullable=True),
        sa.Column('streaming_key_created_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('last_login_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_users_username', 'users', ['username'], unique=True)
    op.create_index('ix_users_streaming_key', 'users', ['streaming_key'], unique=True)
    op.create_index('ix_users_streaming_key_hash', 'users', ['streaming_key_hash'], unique=True)

    op.create_table(
        'user_roles',
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('role_id', sa.Integer(), sa.ForeignKey('roles.id'), nullable=False),
    )

    op.create_table(
        'user_sessions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('token_hash', sa.String(length=128), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('last_used_at', sa.DateTime(), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.Column('revoked', sa.Boolean(), nullable=False, server_default=sa.text('false')),
        sa.Column('user_agent', sa.String(length=255), nullable=True),
        sa.Column('ip_address', sa.String(length=64), nullable=True),
    )
    op.create_index('ix_user_sessions_user_id', 'user_sessions', ['user_id'], unique=False)
    op.create_index('ix_user_sessions_token_hash', 'user_sessions', ['token_hash'], unique=True)

    op.create_table(
        'stream_audit_logs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('event_type', sa.String(length=64), nullable=False),
        sa.Column('endpoint', sa.String(length=255), nullable=True),
        sa.Column('ip_address', sa.String(length=64), nullable=True),
        sa.Column('user_agent', sa.String(length=255), nullable=True),
        sa.Column('details', sa.String(length=1024), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('ix_stream_audit_logs_user_id', 'stream_audit_logs', ['user_id'], unique=False)
    op.create_index('ix_stream_audit_logs_event_type', 'stream_audit_logs', ['event_type'], unique=False)


def downgrade():
    op.drop_index('ix_stream_audit_logs_event_type', table_name='stream_audit_logs')
    op.drop_index('ix_stream_audit_logs_user_id', table_name='stream_audit_logs')
    op.drop_table('stream_audit_logs')

    op.drop_index('ix_user_sessions_token_hash', table_name='user_sessions')
    op.drop_index('ix_user_sessions_user_id', table_name='user_sessions')
    op.drop_table('user_sessions')

    op.drop_table('user_roles')

    op.drop_index('ix_users_streaming_key_hash', table_name='users')
    op.drop_index('ix_users_streaming_key', table_name='users')
    op.drop_index('ix_users_username', table_name='users')
    op.drop_table('users')

    op.drop_index('ix_roles_name', table_name='roles')
    op.drop_table('roles')
