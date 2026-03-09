#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import json
import logging
import re
from dataclasses import dataclass
from urllib.parse import urlencode
import xml.etree.ElementTree as ET

import aiohttp

from backend.plex.runtime import PlexRuntimeServer

logger = logging.getLogger("tic.plex.client")


@dataclass
class PlexResponse:
    status: int
    payload: dict | list | str
    raw_text: str


def _xml_to_object(element: ET.Element):
    result = dict(element.attrib)
    children = list(element)
    if not children:
        text = (element.text or "").strip()
        if text and not result:
            return text
        return result
    for child in children:
        child_value = _xml_to_object(child)
        key = child.tag
        existing = result.get(key)
        if existing is None:
            result[key] = child_value
        elif isinstance(existing, list):
            existing.append(child_value)
        else:
            result[key] = [existing, child_value]
    return result


def parse_plex_payload(text: str):
    body = str(text or "").strip()
    if not body:
        return {}
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        pass
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return {"_raw": body}
    if root.tag == "MediaContainer":
        return {"MediaContainer": _xml_to_object(root)}
    return {root.tag: _xml_to_object(root)}


def get_media_container(payload: dict | list | str) -> dict:
    if isinstance(payload, dict):
        container = payload.get("MediaContainer")
        if isinstance(container, dict):
            return container
    return {}


def ensure_list(value) -> list:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


class PlexClient:
    def __init__(self, server: PlexRuntimeServer, client_identifier: str):
        self.server = server
        self.client_identifier = client_identifier
        self.timeout = aiohttp.ClientTimeout(total=server.timeout_seconds)

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "X-Plex-Token": self.server.token,
            "X-Plex-Client-Identifier": self.client_identifier,
            "X-Plex-Product": "Headendarr",
            "X-Plex-Version": "1.0",
            "X-Plex-Platform": "Python",
            "X-Plex-Platform-Version": "3",
            "X-Plex-Device": "Linux",
            "X-Plex-Device-Name": "Headendarr Plex Reconcile",
            "X-Plex-Language": "en",
            "User-Agent": "headendarr-plex-reconcile/1.0",
        }

    def _build_url(self, path: str, query: dict | None = None) -> str:
        base = self.server.base_url.rstrip("/")
        suffix = path if path.startswith("/") else f"/{path}"
        url = f"{base}{suffix}"
        params = dict(query or {})
        if "X-Plex-Token" not in params:
            params["X-Plex-Token"] = self.server.token
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"
        return url

    async def request(
        self, method: str, path: str, query: dict | None = None, data: bytes | None = None
    ) -> PlexResponse:
        url = self._build_url(path, query)
        async with aiohttp.ClientSession(timeout=self.timeout, headers=self._headers()) as session:
            async with session.request(
                method.upper(),
                url,
                ssl=self.server.verify_tls,
                data=data,
            ) as resp:
                raw_text = await resp.text()
                return PlexResponse(
                    status=int(resp.status),
                    payload=parse_plex_payload(raw_text),
                    raw_text=raw_text,
                )

    async def get_identity(self) -> tuple[str, str]:
        response = await self.request("GET", "/")
        container = get_media_container(response.payload)
        machine_id = str(container.get("machineIdentifier") or "")
        friendly_name = str(container.get("friendlyName") or "")
        if machine_id or friendly_name:
            return machine_id, friendly_name
        match_machine = re.search(r'machineIdentifier="([^"]+)"', response.raw_text or "")
        match_name = re.search(r'friendlyName="([^"]+)"', response.raw_text or "")
        return (
            match_machine.group(1) if match_machine else "",
            match_name.group(1) if match_name else "",
        )

    async def get_devices(self) -> PlexResponse:
        return await self.request("GET", "/media/grabbers/devices")

    async def get_dvrs(self) -> PlexResponse:
        return await self.request("GET", "/livetv/dvrs")

    async def get_lineupchannels(self, lineup_id: str) -> PlexResponse:
        return await self.request("GET", "/livetv/epg/lineupchannels", query={"lineup": lineup_id})

    async def put_device(
        self,
        device_key: str,
        title: str,
        enabled: int = 1,
    ) -> PlexResponse:
        query = {"title": title, "enabled": enabled}
        return await self.request(
            "PUT",
            f"/media/grabbers/devices/{device_key}",
            query=query,
            data=b"",
        )

    async def put_dvr_device(self, dvr_key: str, device_key: str) -> PlexResponse:
        return await self.request("PUT", f"/livetv/dvrs/{dvr_key}/devices/{device_key}", data=b"")

    async def put_device_prefs(self, device_key: str, query: dict) -> PlexResponse:
        return await self.request("PUT", f"/media/grabbers/devices/{device_key}/prefs", query=query, data=b"")

    async def put_dvr_prefs(self, dvr_key: str, query: dict) -> PlexResponse:
        return await self.request("PUT", f"/livetv/dvrs/{dvr_key}/prefs", query=query, data=b"")

    async def put_channelmap(self, device_key: str, payload: dict) -> PlexResponse:
        return await self.request("PUT", f"/media/grabbers/devices/{device_key}/channelmap", query=payload, data=b"")

    async def delete_dvr_device(self, dvr_key: str, device_key: str) -> PlexResponse:
        return await self.request("DELETE", f"/livetv/dvrs/{dvr_key}/devices/{device_key}", data=b"")

    async def create_dvr(
        self,
        device_uuid: str,
        lineup_id: str,
        guide_title: str,
        country: str,
        language: str,
    ) -> PlexResponse:
        return await self.request(
            "POST",
            "/livetv/dvrs",
            query={
                "device": device_uuid,
                "lineup": lineup_id,
                "lineupTitle": guide_title,
                "country": country,
                "language": language,
            },
            data=b"",
        )

    async def try_create_device(self, hdhr_base_url: str, discover_payload: dict) -> bool:
        candidates = [
            ("POST", "/media/grabbers/devices", {"uri": hdhr_base_url}),
            ("PUT", "/media/grabbers/devices", {"uri": hdhr_base_url}),
            ("POST", "/media/grabbers/devices", {"url": hdhr_base_url}),
            (
                "POST",
                "/media/grabbers/devices",
                {
                    "uri": hdhr_base_url,
                    "deviceId": discover_payload.get("DeviceID") or "",
                    "deviceAuth": discover_payload.get("DeviceAuth") or "",
                },
            ),
        ]
        for method, path, query in candidates:
            response = await self.request(method, path, query=query, data=b"")
            if 200 <= response.status < 300:
                return True
            logger.debug("Plex device create attempt failed method=%s path=%s status=%s", method, path, response.status)
        return False
