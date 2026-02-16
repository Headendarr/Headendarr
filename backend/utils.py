#!/usr/bin/env python3
# -*- coding:utf-8 -*-

def normalize_id(value, name):
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {name} id: {value}")


def is_truthy(value):
    if isinstance(value, str):
        return value.lower() in ("1", "true", "yes", "on")
    return bool(value)


def convert_to_int(value, default=1):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
