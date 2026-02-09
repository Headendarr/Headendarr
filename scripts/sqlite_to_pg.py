#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import os
import sys
from datetime import datetime
from pathlib import Path

from urllib.parse import quote_plus

from sqlalchemy import create_engine, MetaData, select, text, inspect


def log(msg):
    print(f"[sqlite_to_pg] {msg}")


def build_pg_url():
    host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "tic")
    user = os.environ.get("POSTGRES_USER", "tic")
    password = os.environ.get("POSTGRES_PASSWORD", "tic")
    password_escaped = quote_plus(password)
    return f"postgresql+psycopg://{user}:{password_escaped}@{host}:{port}/{db}"


def has_pg_data(pg_engine):
    inspector = inspect(pg_engine)
    for table in ("users", "playlists", "epgs", "channels", "recordings"):
        if table in inspector.get_table_names():
            with pg_engine.connect() as conn:
                row = conn.execute(text(f"SELECT 1 FROM {table} LIMIT 1")).fetchone()
                if row is not None:
                    return True
    return False


def copy_table(sqlite_engine, pg_engine, sqlite_table, pg_table, batch_size=1000):
    with sqlite_engine.connect() as src_conn:
        result = src_conn.execute(select(sqlite_table))
        insert_stmt = pg_table.insert()
        rows = result.fetchmany(batch_size)
        while rows:
            payload = [dict(row._mapping) for row in rows]
            with pg_engine.begin() as dst_conn:
                dst_conn.execute(insert_stmt, payload)
            rows = result.fetchmany(batch_size)


def reset_sequences(pg_engine, pg_meta):
    inspector = inspect(pg_engine)
    with pg_engine.begin() as conn:
        for table in pg_meta.sorted_tables:
            if table.name == "alembic_version":
                continue
            pk = inspector.get_pk_constraint(table.name).get("constrained_columns")
            if not pk or len(pk) != 1:
                continue
            pk_col = pk[0]
            seq = conn.execute(
                text("SELECT pg_get_serial_sequence(:t, :c)"),
                {"t": table.name, "c": pk_col},
            ).scalar()
            if not seq:
                continue
            max_id = conn.execute(text(f"SELECT MAX({pk_col}) FROM {table.name}")).scalar()
            if max_id is None:
                max_id = 0
            conn.execute(
                text("SELECT setval(:seq, :val, :is_called)"),
                {"seq": seq, "val": int(max_id), "is_called": bool(max_id)},
            )


def rename_sqlite_db(sqlite_path: Path):
    stamp = datetime.utcnow().strftime("%Y%m%d")
    target = sqlite_path.with_name(f"{sqlite_path.name}.migrated-{stamp}")
    if not target.exists():
        sqlite_path.rename(target)
        log(f"Renamed SQLite DB to {target}")
        return
    # If already exists, append increment
    idx = 1
    while True:
        candidate = sqlite_path.with_name(f"{sqlite_path.name}.migrated-{stamp}.{idx}")
        if not candidate.exists():
            sqlite_path.rename(candidate)
            log(f"Renamed SQLite DB to {candidate}")
            return
        idx += 1


def main():
    sqlite_path = Path(os.environ.get("SQLITE_DB_PATH", "/config/.tvh_iptv_config/db.sqlite3"))
    if not sqlite_path.exists():
        log("SQLite DB not found, skipping migration.")
        return 0

    pg_url = build_pg_url()
    sqlite_url = f"sqlite:///{sqlite_path}"

    log(f"Using SQLite DB: {sqlite_path}")
    log("Connecting to Postgres...")
    pg_engine = create_engine(pg_url)
    sqlite_engine = create_engine(sqlite_url)

    if has_pg_data(pg_engine):
        log("Postgres already has data, skipping migration.")
        return 0

    pg_meta = MetaData()
    sqlite_meta = MetaData()
    pg_meta.reflect(bind=pg_engine)
    sqlite_meta.reflect(bind=sqlite_engine)

    for pg_table in pg_meta.sorted_tables:
        if pg_table.name == "alembic_version":
            continue
        sqlite_table = sqlite_meta.tables.get(pg_table.name)
        if sqlite_table is None:
            continue
        log(f"Migrating table: {pg_table.name}")
        copy_table(sqlite_engine, pg_engine, sqlite_table, pg_table)

    log("Resetting Postgres sequences...")
    reset_sequences(pg_engine, pg_meta)

    # Mark migration complete by renaming SQLite DB
    rename_sqlite_db(sqlite_path)

    # NOTE: This migration script is temporary. Once stable, it can be removed in a future release.
    log("Migration completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
