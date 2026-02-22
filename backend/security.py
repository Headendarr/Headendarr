#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import hashlib
import secrets
from datetime import datetime, timedelta

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from backend.datetime_utils import utc_now_naive


_password_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    return _password_hasher.hash(password)


def verify_password(password_hash: str, password: str) -> bool:
    try:
        return _password_hasher.verify(password_hash, password)
    except VerifyMismatchError:
        return False


def needs_rehash(password_hash: str) -> bool:
    return _password_hasher.check_needs_rehash(password_hash)


def generate_stream_key() -> str:
    # Short, URL-safe stream key (10 chars) per SOW.
    # Use token_urlsafe and truncate for deterministic length.
    return secrets.token_urlsafe(8)[:10]


def hash_stream_key(stream_key: str) -> str:
    return hashlib.sha256(stream_key.encode('utf-8')).hexdigest()


def generate_session_token() -> str:
    return secrets.token_urlsafe(48)


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode('utf-8')).hexdigest()


def compute_session_expiry(days: int = 30) -> datetime:
    return utc_now_naive() + timedelta(days=days)
