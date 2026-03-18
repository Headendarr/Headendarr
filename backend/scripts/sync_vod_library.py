#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import logging
import sys

from sqlalchemy import or_, select

from backend import create_app
from backend.models import Session, VodCategory, User
from backend.vod import (
    load_vod_http_library_index_sync,
    remove_vod_http_library_index_entry_sync,
    remove_vod_http_library_manifest_sync,
    rebuild_vod_group_cache,
    sync_vod_category_strm_files,
    load_vod_strm_registry_sync,
    write_vod_strm_registry_sync,
    remove_vod_export_path_sync,
    eligible_vod_export_users,
    VOD_KIND_MOVIE,
    VOD_KIND_SERIES,
)

logger = logging.getLogger("tic.vod-sync-script")


async def _run(single_category_id: int = None):
    app = create_app()
    config = app.config["APP_CONFIG"]

    async with app.app_context():
        async with Session() as session:
            # Fetch all enabled categories that want library outputs
            stmt = select(VodCategory).where(
                VodCategory.enabled.is_(True),
                or_(VodCategory.generate_strm_files.is_(True), VodCategory.expose_http_library.is_(True)),
            )
            if single_category_id:
                stmt = stmt.where(VodCategory.id == single_category_id)

            result = await session.execute(stmt)
            categories = result.scalars().all()

            if not categories:
                # Just log here. We will continue on to clean up the directory
                logger.info("No VOD categories found for library sync.")

            # Process each category
            for category in categories:
                logger.info("Syncing VOD category: %s (%s)", category.name, category.content_type)
                # Rebuild cache (without queuing another task)
                await rebuild_vod_group_cache(category.id, queue_sync=False)
                # Sync outputs
                await sync_vod_category_strm_files(config, category.id)

            # Global cleanup pass
            logger.info("Performing global VOD export cleanup...")
            registry = load_vod_strm_registry_sync()

            # Get ALL valid current keys across ALL enabled categories
            active_http_category_ids = set()
            stmt_all = select(VodCategory).where(
                VodCategory.enabled.is_(True),
                or_(VodCategory.generate_strm_files.is_(True), VodCategory.expose_http_library.is_(True)),
            )
            res_all = await session.execute(stmt_all)
            active_categories = res_all.scalars().all()

            # Create a set of valid keys for users that have strm files enabled
            all_valid_keys = set()
            for cat in active_categories:
                if bool(getattr(cat, "generate_strm_files", False)):
                    users = await eligible_vod_export_users(cat)
                    cat_suffix = f":{int(cat.id)}"
                    for user in users:
                        all_valid_keys.add(f"{int(user.id)}{cat_suffix}")
                if bool(getattr(cat, "expose_http_library", False)):
                    active_http_category_ids.add(int(cat.id))

            # Remove anything in the registry that isn't valid anymore
            cleaned_count = 0
            for reg_key in list(registry.keys()):
                if reg_key not in all_valid_keys:
                    entry = registry.pop(reg_key)
                    old_path = entry.get("relative_dir")
                    if old_path:
                        logger.info("Cleaning up stale VOD directory: %s", old_path)
                        remove_vod_export_path_sync(old_path)
                        cleaned_count += 1

            if cleaned_count > 0:
                write_vod_strm_registry_sync(registry)
                logger.info("Global cleanup finished. Removed %s stale entries/directories.", cleaned_count)
            else:
                logger.info("Global cleanup finished. Nothing to remove.")

            http_index = load_vod_http_library_index_sync()
            for entry in [row for row in (http_index.get("categories") or []) if isinstance(row, dict)]:
                category_id = int(entry.get("category_id") or 0)
                if category_id <= 0 or category_id in active_http_category_ids:
                    continue
                remove_vod_http_library_index_entry_sync(category_id)
                content_type = str(entry.get("content_type") or "")
                if content_type in {VOD_KIND_MOVIE, VOD_KIND_SERIES}:
                    remove_vod_http_library_manifest_sync(content_type, category_id)


def main():
    single_cat_id = None
    if len(sys.argv) > 1:
        try:
            single_cat_id = int(sys.argv[1])
        except ValueError:
            pass

    try:
        asyncio.run(_run(single_cat_id))
    except Exception as exc:
        print(f"[vod-library-sync] Failed: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
