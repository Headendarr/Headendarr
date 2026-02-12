#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import sys

from backend import create_app
from backend.epgs import build_custom_epg


async def _run():
    app = create_app()
    config = app.config["APP_CONFIG"]
    async with app.app_context():
        await build_custom_epg(config, throttle=False)


def main():
    try:
        asyncio.run(_run())
    except Exception as exc:
        print(f"[epg-build] Failed: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
