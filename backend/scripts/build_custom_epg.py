#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import sys

from backend import create_app
from backend.epgs import build_custom_epg


def main():
    # TODO: Possible ways to speedup:
    # - Replace per-channel programme queries with a single joined query
    # - Stream XML directly to disk instead of building a large in-memory tree
    # - Add/verify DB indexes for programme lookup by channel_id + start
    app = create_app()
    config = app.config['APP_CONFIG']
    try:
        with app.app_context():
            asyncio.run(build_custom_epg(config, throttle=False))
    except Exception as exc:
        print(f"[epg-build] Failed: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
