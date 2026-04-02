#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import ipaddress
import logging
import os
from urllib.parse import urlsplit

from backend.config import flask_run_port

logger = logging.getLogger("tic.url_resolver")


def _env_csv(name: str) -> list[str]:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _normalized_scheme(value: str | None) -> str:
    scheme = (value or "").strip().lower()
    if scheme in {"http", "https"}:
        return scheme
    return "http"


def _normalize_forwarded_prefix(value: str | None) -> str:
    prefix = (value or "").strip()
    if not prefix:
        return ""
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    if prefix != "/":
        prefix = prefix.rstrip("/")
    return prefix


def _normalize_host(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if any(marker in raw for marker in (" ", "/", "\\", "@")):
        return ""
    try:
        parsed = urlsplit(f"//{raw}")
    except Exception:
        return ""
    hostname = (parsed.hostname or "").strip()
    if not hostname:
        return ""
    try:
        port = parsed.port
    except ValueError:
        return ""
    host_display = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    if port:
        return f"{host_display}:{port}"
    return host_display


def _request_peer_ip(request) -> str | None:
    try:
        return (request.remote_addr or "").strip() or None
    except Exception:
        return None


def _trust_forwarded_headers_enabled() -> bool:
    if "TIC_TRUST_PROXY_HEADERS" in os.environ:
        return str(os.environ.get("TIC_TRUST_PROXY_HEADERS", "")).strip().lower() in {"1", "true", "yes", "on"}
    return str(os.environ.get("TIC_TRUST_X_FORWARDED", "true")).strip().lower() in {"1", "true", "yes", "on"}


def _is_trusted_proxy_ip(addr: str | None) -> bool:
    if not addr:
        return False
    cidrs = _env_csv("TIC_TRUSTED_PROXY_CIDRS")
    if not cidrs:
        cidrs = ["127.0.0.1/32", "::1/128"]
    try:
        ip_obj = ipaddress.ip_address(addr)
    except ValueError:
        return False
    for cidr in cidrs:
        try:
            if ip_obj in ipaddress.ip_network(cidr, strict=False):
                return True
        except ValueError:
            continue
    return False


def _trusted_host_match(candidate_host: str, allowed_host: str) -> bool:
    candidate = (candidate_host or "").strip().lower()
    allowed = (allowed_host or "").strip().lower()
    if not candidate or not allowed:
        return False
    if allowed == "*":
        return True
    if ":" not in allowed:
        return candidate == allowed or candidate.startswith(f"{allowed}:")
    return candidate == allowed


def _enforce_trusted_host(candidate_host: str) -> str:
    trusted_hosts = _env_csv("TIC_TRUSTED_HOSTS")
    if not trusted_hosts:
        return candidate_host
    if any(_trusted_host_match(candidate_host, allowed) for allowed in trusted_hosts):
        return candidate_host
    for allowed in trusted_hosts:
        normalized = _normalize_host(allowed)
        if normalized:
            logger.warning(
                "Rejected untrusted Host '%s'; using configured trusted host '%s'.", candidate_host, normalized
            )
            return normalized
    logger.warning("Rejected untrusted Host '%s'; no valid TIC_TRUSTED_HOSTS entries found.", candidate_host)
    return candidate_host


def _request_external_parts(request) -> tuple[str, str, str]:
    scheme = _normalized_scheme(getattr(request, "scheme", "http"))
    host = _normalize_host(getattr(request, "host", ""))
    prefix = _normalize_forwarded_prefix(getattr(request, "root_path", ""))

    trust_forwarded = _trust_forwarded_headers_enabled()
    if trust_forwarded and _is_trusted_proxy_ip(_request_peer_ip(request)):
        headers = request.headers
        forwarded_host = (headers.get("X-Forwarded-Host") or "").split(",", 1)[0].strip()
        forwarded_proto = (headers.get("X-Forwarded-Proto") or "").split(",", 1)[0].strip()
        forwarded_port = (headers.get("X-Forwarded-Port") or "").split(",", 1)[0].strip()
        forwarded_prefix = (headers.get("X-Forwarded-Prefix") or "").split(",", 1)[0].strip()

        normalized_forwarded_host = _normalize_host(forwarded_host)
        if normalized_forwarded_host:
            host = normalized_forwarded_host
            if ":" not in host and forwarded_port.isdigit():
                host = f"{host}:{forwarded_port}"
        if forwarded_proto:
            scheme = _normalized_scheme(forwarded_proto)
        if forwarded_prefix:
            prefix = _normalize_forwarded_prefix(forwarded_prefix)

    host = _enforce_trusted_host(host)
    if not host:
        host = "localhost"
    return scheme, host, prefix


def _request_base_url_diagnostics(request) -> dict[str, str | bool]:
    headers = getattr(request, "headers", {})
    remote_addr = _request_peer_ip(request) or ""
    trusted_proxy = _is_trusted_proxy_ip(remote_addr)
    trust_forwarded = _trust_forwarded_headers_enabled()
    resolved_base_url = get_request_base_url(request)
    return {
        "remote_addr": remote_addr,
        "request_scheme": str(getattr(request, "scheme", "") or ""),
        "request_host": str(getattr(request, "host", "") or ""),
        "request_root_path": str(getattr(request, "root_path", "") or ""),
        "x_forwarded_host": (headers.get("X-Forwarded-Host") or "").split(",", 1)[0].strip(),
        "x_forwarded_proto": (headers.get("X-Forwarded-Proto") or "").split(",", 1)[0].strip(),
        "x_forwarded_port": (headers.get("X-Forwarded-Port") or "").split(",", 1)[0].strip(),
        "x_forwarded_prefix": (headers.get("X-Forwarded-Prefix") or "").split(",", 1)[0].strip(),
        "trusted_proxy_headers_enabled": trust_forwarded,
        "trusted_proxy_cidrs": ",".join(_env_csv("TIC_TRUSTED_PROXY_CIDRS")),
        "proxy_ip_trusted": trusted_proxy,
        "resolved_base_url": resolved_base_url,
    }


def log_request_base_url_diagnostics(request, route_name: str) -> None:
    diag = _request_base_url_diagnostics(request)
    forwarded_proto = str(diag["x_forwarded_proto"] or "")
    forwarded_host = str(diag["x_forwarded_host"] or "")
    forwarded_port = str(diag["x_forwarded_port"] or "")
    forwarded_prefix = str(diag["x_forwarded_prefix"] or "")
    has_forwarded_signal = bool(forwarded_proto or forwarded_host or forwarded_port or forwarded_prefix)
    if not has_forwarded_signal:
        return
    if not diag["trusted_proxy_headers_enabled"]:
        logger.warning(
            "Ignoring X-Forwarded-Proto route=%s remote_addr=%s reason=proxy_header_trust_disabled "
            "x_forwarded_proto=%s x_forwarded_host=%s x_forwarded_port=%s x_forwarded_prefix=%s "
            "request_scheme=%s request_host=%s trusted_proxy_cidrs=%s resolved_base_url=%s env_var=TIC_TRUST_PROXY_HEADERS",
            route_name,
            diag["remote_addr"],
            forwarded_proto,
            forwarded_host,
            forwarded_port,
            forwarded_prefix,
            diag["request_scheme"],
            diag["request_host"],
            diag["trusted_proxy_cidrs"],
            diag["resolved_base_url"],
        )
        return
    if not diag["proxy_ip_trusted"]:
        logger.warning(
            "Ignoring X-Forwarded-Proto route=%s remote_addr=%s reason=proxy_ip_not_trusted "
            "x_forwarded_proto=%s x_forwarded_host=%s x_forwarded_port=%s x_forwarded_prefix=%s "
            "request_scheme=%s request_host=%s trusted_proxy_cidrs=%s resolved_base_url=%s",
            route_name,
            diag["remote_addr"],
            forwarded_proto,
            forwarded_host,
            forwarded_port,
            forwarded_prefix,
            diag["request_scheme"],
            diag["request_host"],
            diag["trusted_proxy_cidrs"],
            diag["resolved_base_url"],
        )
        return
    if forwarded_proto.lower() == "https" and not str(diag["resolved_base_url"]).startswith("https://"):
        logger.warning(
            "Forwarded HTTPS did not resolve to an HTTPS base URL route=%s remote_addr=%s "
            "x_forwarded_proto=%s x_forwarded_host=%s x_forwarded_port=%s x_forwarded_prefix=%s "
            "request_scheme=%s request_host=%s trusted_proxy_cidrs=%s resolved_base_url=%s",
            route_name,
            diag["remote_addr"],
            forwarded_proto,
            forwarded_host,
            forwarded_port,
            forwarded_prefix,
            diag["request_scheme"],
            diag["request_host"],
            diag["trusted_proxy_cidrs"],
            diag["resolved_base_url"],
        )


def get_request_base_url(request) -> str:
    scheme, host, prefix = _request_external_parts(request)
    return f"{scheme}://{host}{prefix}".rstrip("/")


def get_request_origin(request) -> str:
    scheme, host, _ = _request_external_parts(request)
    return f"{scheme}://{host}".rstrip("/")


def get_request_host_info(request) -> tuple[str, str, str]:
    scheme, host, _ = _request_external_parts(request)
    hostname = host
    port = "443" if scheme == "https" else "80"
    if ":" in host and not host.startswith("["):
        hostname, port = host.rsplit(":", 1)
    elif host.startswith("[") and "]:" in host:
        end_idx = host.find("]")
        hostname = host[: end_idx + 1]
        port = host[end_idx + 2 :]
    return hostname, port, scheme


def _normalize_absolute_url(value: str) -> str:
    url = (value or "").strip().rstrip("/")
    if not url:
        return ""
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return url


async def get_tvh_publish_base_url(config) -> str:
    conn_settings = await config.tvh_connection_settings()
    if conn_settings.get("tvh_local"):
        return f"http://127.0.0.1:{flask_run_port}"

    settings = config.read_settings()
    app_url = _normalize_absolute_url(settings.get("settings", {}).get("app_url") or "")
    if app_url:
        return app_url
    raise ValueError("Setting 'app_url' is required when TVHeadend is remote.")
