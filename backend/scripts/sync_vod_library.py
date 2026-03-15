#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import sys
import logging
import shutil
from pathlib import Path

from sqlalchemy import select

from backend import create_app
from backend.models import Session, VodCategory, User
from backend.vod import (
    rebuild_vod_group_cache,
    sync_vod_category_strm_files,
    _load_vod_strm_registry_sync,
    _write_vod_strm_registry_sync,
    _remove_vod_export_path_sync,
    _VOD_STRM_ROOT,
)

logger = logging.getLogger("tic.vod-sync-script")


async def _run(single_category_id: int = None):
    app = create_app()
    config = app.config["APP_CONFIG"]

    async with app.app_context():
        async with Session() as session:
            # 1. Fetch all enabled categories that want STRM files
            stmt = select(VodCategory).where(VodCategory.enabled.is_(True), VodCategory.generate_strm_files.is_(True))
            if single_category_id:
                stmt = stmt.where(VodCategory.id == single_category_id)

            result = await session.execute(stmt)
            categories = result.scalars().all()

            if not categories:
                logger.info("No VOD categories found for sync.")
                # If we were asked to sync a specific category and it's now disabled/missing,
                # we should still proceed to cleanup.

            # 2. Process each category
            processed_keys = set()
            for category in categories:
                logger.info("Syncing VOD category: %s (%s)", category.name, category.content_type)
                # Rebuild cache (without queuing another task)
                await rebuild_vod_group_cache(category.id, queue_sync=False)
                # Sync files
                await sync_vod_category_strm_files(config, category.id)

                # Identify keys that were just processed to protect them from global cleanup
                # Note: sync_vod_category_strm_files updates the registry internally.
                # We'll reload the registry later to see what's currently active.

            # 3. Global Cleanup Pass
            logger.info("Performing global VOD export cleanup...")
            root = _VOD_STRM_ROOT
            registry = _load_vod_strm_registry_sync(root)

            # Get ALL valid current keys across ALL enabled categories
            all_valid_keys = set()
            stmt_all = select(VodCategory).where(
                VodCategory.enabled.is_(True), VodCategory.generate_strm_files.is_(True)
            )
            res_all = await session.execute(stmt_all)
            active_categories = res_all.scalars().all()

            # We also need users to know which keys are valid
            from backend.vod import _eligible_vod_export_users, _vod_safe_name

            active_usernames = set()
            for cat in active_categories:
                users = await _eligible_vod_export_users(cat)
                cat_suffix = f":{int(cat.id)}"
                for user in users:
                    all_valid_keys.add(f"{int(user.id)}{cat_suffix}")
                    active_usernames.add(getattr(user, "username", f"user-{user.id}"))

            # Remove anything in the registry that isn't valid anymore
            cleaned_count = 0
            for reg_key in list(registry.keys()):
                if reg_key not in all_valid_keys:
                    entry = registry.pop(reg_key)
                    old_path = entry.get("relative_dir")
                    if old_path:
                        logger.info("Cleaning up stale VOD directory: %s", old_path)
                        _remove_vod_export_path_sync(old_path, root)
                        cleaned_count += 1

            # AGGRESSIVE: Cleanup legacy lowercase folders for all known users
            # This handles transitions to the new capitalized folder names.
            async with Session() as session_users:
                user_res = await session_users.execute(select(User).where(User.is_active.is_(True)))
                all_possible_users = user_res.scalars().all()
                for u in all_possible_users:
                    user_dir = root / _vod_safe_name(u.username, fallback=f"user-{u.id}")
                    if user_dir.exists():
                        # Remove legacy names if they exist and aren't our new names
                        for legacy in ["movies", "tv-series"]:
                            legacy_dir = user_dir / legacy
                            if legacy_dir.exists():
                                logger.info("Removing legacy lowercase VOD directory: %s", legacy_dir)
                                shutil.rmtree(legacy_dir, ignore_errors=True)
                                cleaned_count += 1

            if cleaned_count > 0:
                _write_vod_strm_registry_sync(registry, root)
                logger.info("Global cleanup finished. Removed %s stale entries/directories.", cleaned_count)
            else:
                logger.info("Global cleanup finished. Nothing to remove.")


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
