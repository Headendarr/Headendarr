#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import os
import sys
import asyncio

# Add the project root to the sys.path before importing backend modules.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.users import get_user_by_username, reset_user_password
from backend.models import Session


async def main():
    print("Attempting to reset admin user password...")
    async with Session() as session:
        admin_user = await get_user_by_username('admin')
        if admin_user:
            await reset_user_password(admin_user.id, 'admin')
            print("Admin user password reset to 'admin'.")
        else:
            print("Admin user not found. Password reset skipped.")

if __name__ == "__main__":
    asyncio.run(main())
