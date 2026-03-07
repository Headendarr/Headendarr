#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import json
from typing import Any
from urllib.parse import urlparse


def clean_xc_host(host_url: str | None) -> str:
    if not host_url:
        return ""
    value = str(host_url).strip().rstrip("/")
    if not value:
        return ""
    if "://" in value:
        parsed = urlparse(value)
        scheme = (parsed.scheme or "").strip()
        netloc = (parsed.netloc or "").strip()
        if scheme and netloc:
            return f"{scheme}://{netloc}"
    return value


def _extract_hosts_from_json(raw_value: str) -> list[str]:
    hosts: list[str] = []
    parsed: Any
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return hosts

    candidate_list = parsed
    if isinstance(parsed, dict):
        candidate_list = parsed.get("hosts") or []
    if not isinstance(candidate_list, list):
        return hosts

    for item in candidate_list:
        enabled = True
        url_value: str | None = None
        if isinstance(item, str):
            url_value = item
        elif isinstance(item, dict):
            enabled = bool(item.get("enabled", True))
            url_value = item.get("url")
        if not enabled:
            continue
        host = clean_xc_host(url_value)
        if host and host not in hosts:
            hosts.append(host)
    return hosts


def parse_xc_hosts(raw_value: str | None) -> list[str]:
    raw = str(raw_value or "").strip()
    if not raw:
        return []

    hosts = _extract_hosts_from_json(raw)
    if hosts:
        return hosts

    parsed_hosts: list[str] = []
    for line in raw.splitlines():
        host = clean_xc_host(line)
        if host and host not in parsed_hosts:
            parsed_hosts.append(host)
    if parsed_hosts:
        return parsed_hosts

    single_host = clean_xc_host(raw)
    return [single_host] if single_host else []


def first_xc_host(raw_value: str | None) -> str:
    hosts = parse_xc_hosts(raw_value)
    if hosts:
        return hosts[0]
    return ""


def serialise_xc_hosts_for_storage(raw_value: str | None) -> str:
    hosts = parse_xc_hosts(raw_value)
    if not hosts:
        return ""
    payload = [
        {
            "url": host,
            "priority": index + 1,
            "enabled": True,
        }
        for index, host in enumerate(hosts)
    ]
    return json.dumps(payload, separators=(",", ":"))


def render_xc_hosts_for_form(raw_value: str | None) -> str:
    return "\n".join(parse_xc_hosts(raw_value))
