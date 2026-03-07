#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import json
import os
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import aiohttp
from authlib.jose import JsonWebKey, jwt


class OidcError(Exception):
    pass


class OidcConfigurationError(OidcError):
    pass


class OidcValidationError(OidcError):
    pass


@dataclass
class OidcConfig:
    enabled: bool
    local_login_enabled: bool
    issuer_url: str
    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: list[str]
    username_claim: str
    email_claim: str
    groups_claim: str
    admin_groups: set[str]
    streamer_groups: set[str]
    default_role: str
    verify_tls: bool
    clock_skew_seconds: int
    auto_provision: bool
    sync_roles_on_login: bool
    button_label: str

    @property
    def configured(self) -> bool:
        return bool(self.issuer_url and self.client_id and self.client_secret and self.redirect_uri)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _env_csv(name: str, default: str = "") -> list[str]:
    raw = os.environ.get(name, default)
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _clean_issuer(issuer_url: str) -> str:
    return str(issuer_url or "").strip().rstrip("/")


def _clean_groups(values: list[str]) -> set[str]:
    return {str(item).strip() for item in values if str(item).strip()}


def load_oidc_config() -> OidcConfig:
    default_scopes = "openid,profile,email,groups"
    default_local_login_enabled = not _env_bool("TIC_AUTH_OIDC_DISABLE_LOCAL_LOGIN", False)
    default_role = str(os.environ.get("TIC_AUTH_OIDC_DEFAULT_ROLE", "none")).strip().lower()
    if default_role not in ("none", "streamer"):
        default_role = "none"

    return OidcConfig(
        enabled=_env_bool("TIC_AUTH_OIDC_ENABLED", False),
        local_login_enabled=_env_bool("TIC_AUTH_LOCAL_LOGIN_ENABLED", default_local_login_enabled),
        issuer_url=_clean_issuer(os.environ.get("TIC_AUTH_OIDC_ISSUER_URL", "")),
        client_id=str(os.environ.get("TIC_AUTH_OIDC_CLIENT_ID", "")).strip(),
        client_secret=str(os.environ.get("TIC_AUTH_OIDC_CLIENT_SECRET", "")).strip(),
        redirect_uri=str(os.environ.get("TIC_AUTH_OIDC_REDIRECT_URI", "")).strip(),
        scopes=_env_csv("TIC_AUTH_OIDC_SCOPES", default_scopes),
        username_claim=str(os.environ.get("TIC_AUTH_OIDC_USERNAME_CLAIM", "preferred_username")).strip(),
        email_claim=str(os.environ.get("TIC_AUTH_OIDC_EMAIL_CLAIM", "email")).strip(),
        groups_claim=str(os.environ.get("TIC_AUTH_OIDC_GROUPS_CLAIM", "groups")).strip(),
        admin_groups=_clean_groups(_env_csv("TIC_AUTH_OIDC_ADMIN_GROUPS", "")),
        streamer_groups=_clean_groups(_env_csv("TIC_AUTH_OIDC_STREAMER_GROUPS", "")),
        default_role=default_role,
        verify_tls=_env_bool("TIC_AUTH_OIDC_VERIFY_TLS", True),
        clock_skew_seconds=max(0, int(str(os.environ.get("TIC_AUTH_OIDC_CLOCK_SKEW_SECONDS", "60")).strip() or 60)),
        auto_provision=_env_bool("TIC_AUTH_OIDC_AUTO_PROVISION", True),
        sync_roles_on_login=_env_bool("TIC_AUTH_OIDC_SYNC_ROLES_ON_LOGIN", True),
        button_label=str(os.environ.get("TIC_AUTH_OIDC_BUTTON_LABEL", "Sign in with SSO")).strip()
        or "Sign in with SSO",
    )


_metadata_cache = {"payload": None, "expires_at": 0.0}
_jwks_cache = {}
_oidc_lock = asyncio.Lock()


async def _fetch_json(url: str, verify_tls: bool) -> dict:
    ssl_value = None if verify_tls else False
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, ssl=ssl_value) as resp:
            body = await resp.text()
            if resp.status >= 400:
                raise OidcError(f"OIDC request failed ({resp.status})")
            try:
                return json.loads(body)
            except json.JSONDecodeError as exc:
                raise OidcError("OIDC endpoint did not return JSON") from exc


async def get_provider_metadata(config: OidcConfig) -> dict:
    if not config.enabled:
        raise OidcConfigurationError("OIDC is not enabled")
    if not config.configured:
        raise OidcConfigurationError("OIDC is enabled but missing required settings")

    now = time.time()
    async with _oidc_lock:
        cached = _metadata_cache.get("payload")
        expires_at = _metadata_cache.get("expires_at", 0.0)
        if cached and expires_at > now:
            return cached

        discovery_url = f"{config.issuer_url}/.well-known/openid-configuration"
        metadata = await _fetch_json(discovery_url, verify_tls=config.verify_tls)
        _metadata_cache["payload"] = metadata
        _metadata_cache["expires_at"] = now + 300
        return metadata


async def get_provider_jwks(config: OidcConfig, metadata: dict) -> dict:
    jwks_uri = str(metadata.get("jwks_uri") or "").strip()
    if not jwks_uri:
        raise OidcError("OIDC provider metadata missing jwks_uri")
    now = time.time()
    async with _oidc_lock:
        cache_entry = _jwks_cache.get(jwks_uri)
        if cache_entry and cache_entry.get("expires_at", 0.0) > now:
            return cache_entry.get("payload")
        jwks = await _fetch_json(jwks_uri, verify_tls=config.verify_tls)
        _jwks_cache[jwks_uri] = {"payload": jwks, "expires_at": now + 300}
        return jwks


def build_authorize_url(config: OidcConfig, metadata: dict, state: str, nonce: str) -> str:
    authorization_endpoint = str(metadata.get("authorization_endpoint") or "").strip()
    if not authorization_endpoint:
        raise OidcError("OIDC provider metadata missing authorization_endpoint")
    params = {
        "response_type": "code",
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
        "scope": " ".join(config.scopes),
        "state": state,
        "nonce": nonce,
    }
    return f"{authorization_endpoint}?{urlencode(params)}"


async def exchange_code_for_tokens(config: OidcConfig, metadata: dict, code: str) -> dict:
    token_endpoint = str(metadata.get("token_endpoint") or "").strip()
    if not token_endpoint:
        raise OidcError("OIDC provider metadata missing token_endpoint")

    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    basic = base64.b64encode(f"{config.client_id}:{config.client_secret}".encode("utf-8")).decode("ascii")
    headers["Authorization"] = f"Basic {basic}"

    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": config.redirect_uri,
    }

    ssl_value = None if config.verify_tls else False
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(token_endpoint, data=payload, headers=headers, ssl=ssl_value) as resp:
            body = await resp.text()
            if resp.status >= 400:
                raise OidcError(f"OIDC token exchange failed ({resp.status})")
            try:
                token_payload = json.loads(body)
            except json.JSONDecodeError as exc:
                raise OidcError("OIDC token endpoint did not return JSON") from exc

    if "id_token" not in token_payload:
        raise OidcError("OIDC token response missing id_token")
    return token_payload


def _claim_by_path(claims: dict, claim_path: str):
    current = claims
    for part in str(claim_path or "").split("."):
        part = part.strip()
        if not part:
            continue
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def extract_claim_value(claims: dict, claim_name: str):
    if not claim_name:
        return None
    if "." in claim_name:
        value = _claim_by_path(claims, claim_name)
        if value is not None:
            return value
    return claims.get(claim_name)


def extract_groups(claims: dict, groups_claim: str) -> set[str]:
    value = extract_claim_value(claims, groups_claim)
    if value is None:
        return set()
    if isinstance(value, list):
        return {str(item).strip() for item in value if str(item).strip()}
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return set()
        if "," in raw:
            return {item.strip() for item in raw.split(",") if item.strip()}
        return {raw}
    return set()


def map_roles_from_claims(claims: dict, config: OidcConfig) -> list[str]:
    groups = extract_groups(claims, config.groups_claim)
    role_names = set()
    if config.admin_groups and groups.intersection(config.admin_groups):
        role_names.add("admin")
    if config.streamer_groups and groups.intersection(config.streamer_groups):
        role_names.add("streamer")
    if not role_names and config.default_role == "streamer":
        role_names.add("streamer")
    return sorted(role_names)


def resolve_username_from_claims(claims: dict, config: OidcConfig) -> str:
    for key in (
        config.username_claim,
        "preferred_username",
        "email",
        "name",
        "sub",
    ):
        value = extract_claim_value(claims, key)
        if value is None:
            continue
        value_str = str(value).strip()
        if value_str:
            return value_str[:64]
    return "oidc-user"


async def fetch_userinfo_claims(config: OidcConfig, metadata: dict, access_token: str) -> dict:
    userinfo_endpoint = str(metadata.get("userinfo_endpoint") or "").strip()
    if not userinfo_endpoint or not access_token:
        return {}
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    ssl_value = None if config.verify_tls else False
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(userinfo_endpoint, headers=headers, ssl=ssl_value) as resp:
            body = await resp.text()
            if resp.status >= 400:
                return {}
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                return {}
            return payload if isinstance(payload, dict) else {}


async def validate_and_build_claims(config: OidcConfig, metadata: dict, tokens: dict, nonce: str) -> dict:
    nonce = str(nonce or "").strip()
    if not nonce:
        raise OidcValidationError("Missing nonce")

    id_token = str(tokens.get("id_token") or "").strip()
    if not id_token:
        raise OidcValidationError("Missing id_token")

    jwks = await get_provider_jwks(config, metadata)
    key_set = JsonWebKey.import_key_set(jwks)
    claims = jwt.decode(id_token, key_set)
    claims.validate(leeway=config.clock_skew_seconds)

    payload = dict(claims)
    issuer = _clean_issuer(payload.get("iss"))
    if issuer != config.issuer_url:
        raise OidcValidationError("Issuer mismatch")

    aud = payload.get("aud")
    if isinstance(aud, list):
        if config.client_id not in aud:
            raise OidcValidationError("Audience mismatch")
    elif aud != config.client_id:
        raise OidcValidationError("Audience mismatch")

    token_nonce = str(payload.get("nonce") or "").strip()
    if not token_nonce:
        raise OidcValidationError("Missing token nonce")
    if token_nonce != nonce:
        raise OidcValidationError("Nonce mismatch")

    access_token = str(tokens.get("access_token") or "").strip()
    userinfo_claims = await fetch_userinfo_claims(config, metadata, access_token)
    merged_claims = dict(payload)
    if userinfo_claims:
        userinfo_sub = str(userinfo_claims.get("sub") or "").strip()
        id_token_sub = str(payload.get("sub") or "").strip()
        if userinfo_sub and userinfo_sub != id_token_sub:
            raise OidcValidationError("Userinfo subject mismatch")

        protected_claims = {"sub", "iss", "aud", "exp", "iat", "nbf", "nonce", "azp", "auth_time", "jti"}
        for key, value in userinfo_claims.items():
            if key in protected_claims:
                continue
            merged_claims[key] = value

    subject = str(merged_claims.get("sub") or "").strip()
    if not subject:
        raise OidcValidationError("Missing subject")
    return merged_claims
