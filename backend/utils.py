#!/usr/bin/env python3
# -*- coding:utf-8 -*-

def normalize_id(value, name):
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {name} id: {value}")
