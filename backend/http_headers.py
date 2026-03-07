#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import base64
import json
import re


_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_DISALLOWED_LOWER_HEADERS = {
    "host",
    "connection",
    "content-length",
    "transfer-encoding",
    "upgrade",
    "keep-alive",
    "te",
    "trailer",
    "proxy-authenticate",
    "proxy-authorization",
}
_MAX_HEADERS = 32
_MAX_HEADER_NAME_LENGTH = 128
_MAX_HEADER_VALUE_LENGTH = 2048
_MAX_QUERY_PAYLOAD_LENGTH = 8192


def _canonical_header_name(name: str) -> str:
    return "-".join(part[:1].upper() + part[1:].lower() for part in name.split("-") if part)


def sanitise_headers(raw_headers, raise_on_invalid=False):
    if not raw_headers:
        return {}
    if not isinstance(raw_headers, dict):
        if raise_on_invalid:
            raise ValueError("Headers must be a JSON object.")
        return {}

    output = {}
    for raw_name, raw_value in raw_headers.items():
        name = str(raw_name or "").strip()
        if not name:
            if raise_on_invalid:
                raise ValueError("Header names must not be empty.")
            continue
        if len(name) > _MAX_HEADER_NAME_LENGTH or not _HEADER_NAME_RE.match(name):
            if raise_on_invalid:
                raise ValueError(f"Invalid header name: '{name}'.")
            continue
        if name.lower() in _DISALLOWED_LOWER_HEADERS:
            if raise_on_invalid:
                raise ValueError(f"Header '{name}' is not allowed.")
            continue
        if raw_value is None:
            continue
        value = str(raw_value).strip()
        if not value:
            continue
        if len(value) > _MAX_HEADER_VALUE_LENGTH:
            if raise_on_invalid:
                raise ValueError(f"Header '{name}' value is too long.")
            continue
        output[_canonical_header_name(name)] = value
        if len(output) >= _MAX_HEADERS:
            break

    return output


def parse_headers_json(value):
    if value is None:
        return {}
    payload = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid headers JSON: {exc.msg}.") from exc
    return sanitise_headers(payload, raise_on_invalid=True)


def serialise_headers_json(headers):
    clean = sanitise_headers(headers, raise_on_invalid=True)
    if not clean:
        return None
    return json.dumps(clean, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def merge_headers(preferred=None, fallback=None):
    merged = {}
    if fallback:
        merged.update(sanitise_headers(fallback))
    if preferred:
        merged.update(sanitise_headers(preferred))
    return merged


def ensure_user_agent(headers, user_agent):
    clean = sanitise_headers(headers)
    ua = str(user_agent or "").strip()
    if not ua:
        return clean
    lower_keys = {key.lower() for key in clean.keys()}
    if "user-agent" not in lower_keys:
        clean["User-Agent"] = ua
    return clean


def encode_headers_query_param(headers):
    clean = sanitise_headers(headers)
    if not clean:
        return None
    payload = json.dumps(clean, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    encoded = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")
    if len(encoded) > _MAX_QUERY_PAYLOAD_LENGTH:
        raise ValueError("Encoded headers are too large for URL usage.")
    return encoded


def decode_headers_query_param(value):
    token = str(value or "").strip()
    if not token:
        return {}
    if len(token) > _MAX_QUERY_PAYLOAD_LENGTH:
        return {}
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        payload = json.loads(raw)
    except Exception:
        return {}
    return sanitise_headers(payload)
