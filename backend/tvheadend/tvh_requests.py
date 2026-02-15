#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import json
import logging
import os
import re
import aiohttp
import asyncio

from backend.config import flask_run_port
from backend.dvr_profiles import normalize_retention_policy, retention_policy_to_tvh_days
from backend.streaming import (
    LOCAL_PROXY_HOST_PLACEHOLDER,
    append_stream_key,
    get_tvh_stream_auth,
)

logger = logging.getLogger('tic.tvh_requests')

_SAFE_SEGMENT_RE = re.compile(r"[^A-Za-z0-9._-]")


def _build_user_scoped_pathname(username: str, pathname: str) -> str:
    safe_username = _SAFE_SEGMENT_RE.sub("_", str(username or "user")).strip("._-") or "user"
    raw_pathname = str(pathname or "").strip()
    normalized = raw_pathname.lstrip("/")
    if not normalized:
        normalized = "$t/$t-$c-%F_%R$n.$x"
    return f"{safe_username}/{normalized}"

# TVheadend API URLs:
api_config_save = "config/save"
api_imagecache_config_save = "imagecache/config/save"
api_accessentry_grid = "access/entry/grid"
api_accessentry_config_create = "access/entry/create"
api_password_grid = "passwd/entry/grid"
api_password_config_create = "passwd/entry/create"
api_epggrab_config_save = "epggrab/config/save"
api_timeshift_config_save = "/timeshift/config/save"
api_list_scanfile = "dvb/scanfile/list"
api_create_network = "mpegts/network/create"
api_view_networks = "mpegts/network/grid"
api_view_network_builders = "mpegts/network/builders"
api_network_mux_class = "mpegts/network/mux_class"
api_create_mux = "mpegts/network/mux_create"
api_input_networks = "mpegts/input/network_list"
api_view_muxes = "mpegts/mux/grid"
api_list_all_services = "mpegts/service/grid"
api_idnode_load = "idnode/load"
api_idnode_save = "idnode/save"
api_idnode_delete = "idnode/delete"
api_hardware_tree = "hardware/tree"
api_list_all_channel_tags = "channeltag/grid"
api_create_channel_tag = "channeltag/create"
api_list_all_channels = "channel/grid"
api_create_channel = "channel/create"
api_services_mapper = "service/mapper/save"
api_services_list = "service/list"
api_int_epggrab_run = "epggrab/internal/rerun"
api_epggrab_list = "epggrab/module/list"
api_dvr_entry_grid = "dvr/entry/grid"
api_dvr_entry_create = "dvr/entry/create"
api_dvr_entry_cancel = "dvr/entry/cancel"
api_dvr_config_create = "dvr/config/create"
api_status_subscriptions = "status/subscriptions"
api_status_subscriptions_grid = "status/subscriptions/grid"

tvh_config = {
    "server_name":    "TVH-IPTV",
    "uilevel":        2,
    "digest":         0,
    "parser_backlog": True,  # See link above default_stream_profile_template config below
}
tvh_imagecache_config = {"enabled": False, "ignore_sslcert": True, "expire": 7, "ok_period": 168, "fail_period": 24}
tvh_client_access_entry_comment = "TVH IPTV Config client access entry"
tvh_client_password_comment = "TVH IPTV Config client password entry"
tvh_sync_access_entry_comment = "TVH IPTV Config sync access entry"
tvh_sync_password_comment = "TVH IPTV Config sync password entry"
tvh_user_access_comment_prefix = "TVH IPTV Config user access entry"
tvh_user_password_comment_prefix = "TVH IPTV Config user password entry"
tvh_client_access_entry = {
    "comment":             "COMMENT",
    "enabled":             True,
    "username":            "user",
    "change":              [
        "change_rights",
        "change_chrange",
        "change_chtags",
        "change_dvr_configs",
        "change_profiles",
        "change_conn_limit",
        "change_uilevel",
        "change_xmltv_output",
        "change_htsp_output"
    ],
    "webui":               True,
    "admin":               False,
    "streaming":           ["basic", "advanced", "htsp"],
    "dvr":                 ["basic", "htsp", "all"],
    "prefix":              "0.0.0.0/0,::/0",
    "lang":                "",
    "themeui":             "",
    "langui":              "",
    "profile":             "",
    "dvr_config":          "",
    "channel_min":         "0",
    "channel_max":         "0",
    "channel_tag_exclude": False,
    "channel_tag":         "",
    "xmltv_output_format": 0,
    "htsp_output_format":  0,
    "uilevel":             0,
    "uilevel_nochange":    0,
    "conn_limit_type":     0,
    "conn_limit":          0,
    "htsp_anonymize":      False
}
tvh_admin_access_entry_comment = "TVH IPTV Config admin access entry"
tvh_admin_password_comment = "TVH IPTV Config admin password entry"
tvh_admin_access_entry = {
    "comment":             "COMMENT",
    "enabled":             True,
    "username":            "admin",
    "change":              [
        "change_rights", "change_chrange",
        "change_chtags", "change_dvr_configs",
        "change_profiles", "change_conn_limit",
        "change_lang", "change_lang_ui",
        "change_theme", "change_uilevel",
        "change_xmltv_output", "change_htsp_output"
    ],
    "webui":               True,
    "admin":               True,
    "streaming":           ["basic", "advanced", "htsp"],
    "dvr":                 ["basic", "htsp"],
    "prefix":              "0.0.0.0/0,::/0",
    "lang":                "",
    "themeui":             "",
    "langui":              "",
    "profile":             [],
    "dvr_config":          [],
    "channel_min":         "0",
    "channel_max":         "0",
    "channel_tag_exclude": False,
    "channel_tag":         [],
    "xmltv_output_format": 0,
    "htsp_output_format":  0,
    "uilevel":             -1,
    "uilevel_nochange":    -1,
    "conn_limit_type":     0,
    "conn_limit":          0,
    "htsp_anonymize":      False
}
tvh_u_password = {
    "comment":  "COMMENT",
    "username": "USERNAME",
    "password": "PASSWORD",
    "enabled":  True,
    "auth":     ["enable"],
}
epggrab_config = {
    "channel_rename":     False, "channel_renumber": False, "channel_reicon": False,
    "epgdb_periodicsave": 0, "epgdb_saveafterimport": True,
    "int_initial":        True, "cron": "25 */12 * * *",
    "ota_initial":        False, "ota_cron": ""
}
network_template = {
    "enabled":      True,
    "networkname":  'NETWORK_NAME',
    "pnetworkname": 'PROVIDER_NETWORK_NAME',
    "max_streams":  1,
    "priority":     1,
    "max_timeout":  30,
    "skipinitscan": True,
    "bouquet":      False, "max_bandwidth": 0, "nid": 0, "ignore_chnum": True, "satip_source": 0,
    "charset":      "", "use_libav": False, "scan_create": False, "spriority": 1, "icon_url": "",
    "idlescan":     False, "sid_chnum": False, "localtime": 0, "service_sid": 0, "remove_scrambled": True
}
channel_tag_comment = "TVH IPTV Config channel tag"
channel_tag_template = {
    "enabled":     True,
    "name":        "TAG_NAME",
    "comment":     "TAG_COMMENT",
    "index":       0,
    "internal":    False,
    "icon":        "",
    "private":     False,
    "titled_icon": False
}
channel_template = {
    "enabled":      True,
    "name":         "CHANNEL_NAME",
    "number":       "CHANNEL_NUMBER",
    "tags":         "CHANNEL_TAGS",
    "icon":         "CHANNEL_ICON",
    "services":     "", "autoname": False, "epgauto": True, "epglimit": 0, "epggrab": "", "dvr_pre_time": 0,
    "dvr_pst_time": 0, "remote_timeshift": False, "epg_running": 0, "epg_parent": ""
}
mux_template = {
    "iptv_url":          "IPTV_URL",
    "iptv_icon":         "ICON",
    "iptv_sname":        "SERVICE_NAME",
    "iptv_muxname":      "NETWORK - SERVICE_NAME",
    "channel_number":    "0",
    "iptv_epgid":        "0",
    "iptv_tags":         "",
    "enabled":           0,
    "epg":               1, "epg_module_id": "", "use_libav": 0, "iptv_atsc": False, "scan_state": 0, "charset": "",
    "priority":          0, "spriority": 0, "iptv_send_reports": False, "iptv_ret_url": "", "iptv_substitute": False,
    "iptv_interface":    "", "iptv_satip_dvbt_freq": 0, "iptv_satip_dvbc_freq": 0, "iptv_satip_dvbs_freq": 0,
    "iptv_buffer_limit": 0, "tsid_zero": False, "pmt_06_ac3": 0, "eit_tsid_nocheck": False, "sid_filter": 0,
    "iptv_respawn":      False, "iptv_kill": 0, "iptv_kill_timeout": 5, "iptv_env": "", "iptv_hdr": ""
}
# REF: https://tvheadend.org/d/5282-iptv-buffering-stalls-general-drop-outs-settings-that-resolved-things-for-me/41
default_stream_profile_template = {
    "enabled":     True,
    "default":     True,
    "comment":     "MPEG-TS Pass-thru",
    "timeout":     0,
    "priority":    3,
    "fpriority":   0,
    "restart":     True,
    "contaccess":  True,
    "catimeout":   2000,
    "swservice":   True,
    "svfilter":    0,
    "sid":         0,
    "rewrite_pmt": False,
    "rewrite_pat": False,
    "rewrite_sdt": False,
    "rewrite_nit": False,
    "rewrite_eit": False,
}
htsp_stream_profile_template = {
    "enabled":    True,
    "default":    False,
    "comment":    "HTSP Default Stream Settings",
    "timeout":    0,
    "priority":   3,
    "fpriority":  0,
    "restart":    True,
    "contaccess": True,
    "catimeout":  2000,
    "swservice":  True,
    "svfilter":   0,
}

default_recorder_profile_template = {
    "pre-extra-time":               2,
    "post-extra-time":              5,
    "pathname":                     "$t/$t-$c-%F_%R$n.$x",
    "comment":                      "",
    "storage":                      "/recordings",
    "format-tvmovies-subdir":       "tvmovies",
    "format-tvshows-subdir":        "tvshows",
    "clean-title":                  True,
    "whitespace-in-title":          True,
    "windows-compatible-filenames": False,
    "skip-commercials":             False
}

default_timeshift_config_template = {
    "enabled":          True,
    "max_period":       60,
    "path":             "/timeshift",
    "max_size":         10000,
    "ram_size":         0,
    "ram_only":         False,
    "ondemand":         False,
    "unlimited_period": False,
    "unlimited_size":   False,
    "ram_fit":          False,
    "teletext":         False
}


class Tvheadend:

    def __init__(self, host, port, path, admin_username, admin_password, local_conn):
        self.api_url = f"http://{host}:{port}{path}/api"
        self.admin_username = admin_username
        self.admin_password = admin_password
        self.local_conn = local_conn
        self.timeout = 5
        self.session = aiohttp.ClientSession(auth=aiohttp.BasicAuth(admin_username,
                                                                    admin_password)) if admin_username and admin_password else aiohttp.ClientSession()
        self.default_headers = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def __get(self, url, payload=None, rformat='content'):
        headers = self.default_headers
        async with self.session.get(url, headers=headers, params=payload, allow_redirects=False,
                                    timeout=self.timeout) as r:
            if r.status == 200:
                if rformat == 'json':
                    return await r.json(content_type=None)
                return await r.text()
            raise Exception(f"GET Failed to TVH API - CODE:{r.status} - CONTENT:{await r.text()} - PAYLOAD:{payload}")

    async def __post(self, url, payload=None, rformat='content'):
        headers = self.default_headers
        async with self.session.post(url, headers=headers, data=payload, allow_redirects=False,
                                     timeout=self.timeout) as r:
            if r.status == 200:
                if rformat == 'json':
                    return await r.json(content_type=None)
                return await r.text()
            raise Exception(f"POST Failed to TVH API - CODE:{r.status} - CONTENT:{await r.text()} - PAYLOAD:{payload}")

    async def __json(self, url, payload=None):
        headers = self.default_headers
        headers['Content-Type'] = 'application/json'
        async with self.session.post(url, headers=headers, json=payload, allow_redirects=False,
                                     timeout=self.timeout) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            raise Exception(f"JSON Failed to TVH API - CODE:{r.status} - CONTENT:{await r.text()}")

    async def idnode_load(self, data):
        url = f"{self.api_url}/{api_idnode_load}"
        response = await self.__post(url, payload=data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        return json_list

    async def idnode_save(self, node):
        url = f"{self.api_url}/{api_idnode_save}"
        await self.__post(url, payload={"node": json.dumps(node)})

    async def list_dvr_entries(self):
        url = f"{self.api_url}/{api_dvr_entry_grid}"
        response = await self.__post(url, payload={"limit": 2000, "sort": "start", "dir": "ASC"})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        entries = []
        for entry in json_list.get("entries", []):
            title = None
            if isinstance(entry.get("title"), dict):
                title = next(iter(entry.get("title").values()), None)
            entries.append({
                "uuid": entry.get("uuid"),
                "channel": entry.get("channel"),
                "channelname": entry.get("channelname"),
                "start": entry.get("start"),
                "stop": entry.get("stop"),
                "state": entry.get("status") or entry.get("state"),
                "title": entry.get("disp_title") or title,
                "description": entry.get("disp_description") or entry.get("disp_summary"),
            })
        return entries

    async def list_status_subscriptions(self):
        """Return current TVH subscriptions from status API across TVH versions."""
        # TVH APIs vary across versions: try legacy + grid variants.
        attempts = (
            ("GET", f"{self.api_url}/{api_status_subscriptions}", {"all": 1, "limit": 999999}),
            ("POST", f"{self.api_url}/{api_status_subscriptions}", {"all": 1, "limit": 999999}),
            ("POST", f"{self.api_url}/{api_status_subscriptions_grid}", {"all": 1, "limit": 999999}),
            ("GET", f"{self.api_url}/{api_status_subscriptions_grid}", {"all": 1, "limit": 999999}),
        )
        last_exc = None
        for method, url, payload in attempts:
            try:
                if method == "GET":
                    result = await self.__get(url, payload=payload, rformat="json")
                else:
                    result = await self.__post(url, payload=payload, rformat="json")
                entries = result.get("entries") if isinstance(result, dict) else []
                if isinstance(entries, list):
                    return entries
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc:
            raise last_exc
        return []

    async def create_dvr_entry(self, channel_uuid, start_ts, stop_ts, title, description=None, config_name=""):
        conf = {
            "channel": channel_uuid,
            "start": int(start_ts),
            "stop": int(stop_ts),
            "title": {"eng": title},
            "config_name": config_name or "",
        }
        if description:
            conf["description"] = {"eng": description}
            conf["comment"] = description
        url = f"{self.api_url}/{api_dvr_entry_create}"
        response = await self.__post(url, payload={"conf": json.dumps(conf)})
        try:
            json_data = json.loads(response)
        except json.JSONDecodeError:
            json_data = {}
        return json_data.get("uuid")

    async def delete_dvr_entry(self, uuid):
        url = f"{self.api_url}/{api_idnode_delete}"
        payload = {"uuid": json.dumps([uuid])}
        try:
            await self.__post(url, payload=payload)
            return
        except Exception:
            pass
        try:
            # Fallback to JSON list payload if form encoding fails for this TVH version.
            await self.__json(url, payload=[uuid])
            return
        except Exception:
            pass
        # Final fallback: JSON object payload with uuid list
        await self.__json(url, payload={"uuid": [uuid]})

    async def idnode_delete(self, node_uuid):
        url = f"{self.api_url}/{api_idnode_delete}"
        await self.__post(url, payload={"uuid": node_uuid})

    async def save_tvh_config(self, node):
        url = f"{self.api_url}/{api_config_save}"
        await self.__post(url, payload={"node": json.dumps(node)})

    async def save_imagecache_config(self, node):
        url = f"{self.api_url}/{api_imagecache_config_save}"
        await self.__post(url, payload={"node": json.dumps(node)})

    async def create_accessentry_config(self, node):
        url = f"{self.api_url}/{api_imagecache_config_save}"
        await self.__post(url, payload={"node": json.dumps(node)})

    async def save_epggrab_config(self, node):
        url = f"{self.api_url}/{api_epggrab_config_save}"
        await self.__post(url, payload={"node": json.dumps(node)})

    async def disable_all_epg_grabbers(self):
        url = f"{self.api_url}/{api_epggrab_list}"
        response = await self.__get(url, payload={}, rformat='json')
        for grabber in response.get('entries', []):
            await self.idnode_save({"enabled": False, "uuid": grabber['uuid']})

    async def create_and_configure_client_user(self, username, password):
        # Read current access entries
        url = f"{self.api_url}/{api_accessentry_grid}"
        response = await self.__post(url, payload={'groupBy': 'false', 'groupDir': 'ASC'})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        # Create template for access entry
        node = tvh_client_access_entry.copy()
        user_comment = tvh_client_access_entry_comment
        node['comment'] = user_comment
        for entry in json_list.get('entries', []):
            if entry.get('comment') == user_comment:
                node['uuid'] = entry['uuid']
        # Create the access control if it does not yet exist
        if not node.get('uuid'):
            # Create access control
            post_data = {"conf": json.dumps(node)}
            # Send request to server
            url = f"{self.api_url}/{api_accessentry_config_create}"
            response = await self.__post(url, payload=post_data)
            try:
                json_list = json.loads(response)
            except json.JSONDecodeError:
                json_list = {}
            node['uuid'] = json_list.get('uuid')
        # Save and update the client user access control
        node['username'] = username
        node['comment'] = user_comment
        await self.idnode_save(node)
        # Read current password entries
        url = f"{self.api_url}/{api_password_grid}"
        response = await self.__post(url, payload={'groupBy': 'false', 'groupDir': 'ASC'})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        # Create template for password
        node = tvh_u_password.copy()
        pass_comment = tvh_client_password_comment
        node['comment'] = pass_comment
        for entry in json_list.get('entries', []):
            if entry.get('comment') == pass_comment:
                node['uuid'] = entry['uuid']
        # Create the password if it does not yet exist
        if not node.get('uuid'):
            # Create access control
            post_data = {"conf": json.dumps(node)}
            # Send request to server
            url = f"{self.api_url}/{api_password_config_create}"
            response = await self.__post(url, payload=post_data)
            try:
                json_list = json.loads(response)
            except json.JSONDecodeError:
                json_list = {}
            node['uuid'] = json_list.get('uuid')
        # Save and update the client user access control
        node['username'] = username
        node['password'] = password
        node['comment'] = pass_comment
        await self.idnode_save(node)

    async def update_admin_user_password(self, password):
        # Read current password entries
        url = f"{self.api_url}/{api_password_grid}"
        response = await self.__post(url, payload={'groupBy': 'false', 'groupDir': 'ASC'})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        # Create template for password
        node = tvh_u_password.copy()
        pass_comment = tvh_admin_password_comment
        node['comment'] = pass_comment
        for entry in json_list.get('entries', []):
            if entry.get('comment') == pass_comment:
                node['uuid'] = entry['uuid']
        # Create the password if it does not yet exist
        if not node.get('uuid'):
            # Create access control
            post_data = {"conf": json.dumps(node)}
            # Send request to server
            url = f"{self.api_url}/{api_password_config_create}"
            response = await self.__post(url, payload=post_data)
            try:
                json_list = json.loads(response)
            except json.JSONDecodeError:
                json_list = {}
            node['uuid'] = json_list.get('uuid')
        # Save and update the client user access control
        node['username'] = 'admin'
        node['password'] = password
        node['comment'] = pass_comment
        await self.idnode_save(node)

    async def remove_client_user(self):
        # Read current access entries
        url = f"{self.api_url}/{api_accessentry_grid}"
        response = await self.__post(url, payload={'groupBy': 'false', 'groupDir': 'ASC'})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        # Look for the client user access entry managed by TIC
        access_entry_uuid = None
        for entry in json_list.get('entries', []):
            if entry.get('comment') == tvh_client_access_entry_comment:
                access_entry_uuid = entry['uuid']
        # Read current password entries
        url = f"{self.api_url}/{api_password_grid}"
        response = await self.__post(url, payload={'groupBy': 'false', 'groupDir': 'ASC'})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        # Look for the client password managed by TIC
        password_uuid = None
        for entry in json_list.get('entries', []):
            if entry.get('comment') == tvh_client_password_comment:
                password_uuid = entry['uuid']
        # Delete idnodes
        if access_entry_uuid:
            await self.idnode_delete(access_entry_uuid)
        if password_uuid:
            await self.idnode_delete(password_uuid)

    async def _get_access_entries(self):
        url = f"{self.api_url}/{api_accessentry_grid}"
        response = await self.__post(url, payload={'groupBy': 'false', 'groupDir': 'ASC'})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        return json_list.get("entries", [])

    async def _get_password_entries(self):
        url = f"{self.api_url}/{api_password_grid}"
        response = await self.__post(url, payload={'groupBy': 'false', 'groupDir': 'ASC'})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        return json_list.get("entries", [])

    async def upsert_user(
        self,
        username,
        password,
        is_admin=False,
        enabled=True,
        access_comment=None,
        password_comment=None,
        dvr_config=None,
        dvr_permissions=None,
    ):
        access_comment = access_comment or f"{tvh_user_access_comment_prefix}:{username}"
        password_comment = password_comment or f"{tvh_user_password_comment_prefix}:{username}"

        access_entries = await self._get_access_entries()
        password_entries = await self._get_password_entries()

        access_node = (tvh_admin_access_entry if is_admin else tvh_client_access_entry).copy()
        access_node["comment"] = access_comment
        existing_access = next((entry for entry in access_entries if entry.get("comment") == access_comment), None)
        if existing_access:
            access_node["uuid"] = existing_access["uuid"]

        if not access_node.get("uuid"):
            post_data = {"conf": json.dumps(access_node)}
            url = f"{self.api_url}/{api_accessentry_config_create}"
            response = await self.__post(url, payload=post_data)
            try:
                json_list = json.loads(response)
            except json.JSONDecodeError:
                json_list = {}
            access_node["uuid"] = json_list.get("uuid")

        access_node["username"] = username
        access_node["enabled"] = enabled
        access_node["admin"] = True if is_admin else False
        access_node["webui"] = True if is_admin else False
        if dvr_config is not None:
            access_node["dvr_config"] = dvr_config
        if dvr_permissions is not None:
            access_node["dvr"] = dvr_permissions
        await self.idnode_save(access_node)

        password_node = tvh_u_password.copy()
        password_node["comment"] = password_comment
        existing_password = next((entry for entry in password_entries if entry.get("comment") == password_comment), None)
        if existing_password:
            password_node["uuid"] = existing_password["uuid"]
        if not password_node.get("uuid"):
            post_data = {"conf": json.dumps(password_node)}
            url = f"{self.api_url}/{api_password_config_create}"
            response = await self.__post(url, payload=post_data)
            try:
                json_list = json.loads(response)
            except json.JSONDecodeError:
                json_list = {}
            password_node["uuid"] = json_list.get("uuid")

        password_node["username"] = username
        password_node["password"] = password
        password_node["enabled"] = enabled
        await self.idnode_save(password_node)

    async def enable_xmltv_url_epg_grabber(self, epg_url):
        url = f"{self.api_url}/{api_epggrab_list}"
        response = await self.__get(url, payload={}, rformat='json')
        for grabber in response.get('entries', []):
            if grabber['title'] == "Internal: XMLTV: XMLTV URL grabber":
                node = {
                    "uuid":     grabber['uuid'],
                    "args":     epg_url,
                    "enabled":  True,
                    "priority": 1,
                    "dn_chnum": 0,
                }
                await self.idnode_save(node)

    async def configure_default_stream_profile(self):
        response = await self.idnode_load({'enum': 1, 'class': 'profile'})
        for profile in response.get('entries', []):
            if profile['val'] == "pass":
                node = default_stream_profile_template.copy()
                node['uuid'] = profile['key']
                await self.idnode_save(node)

    async def configure_htsp_stream_profile(self):
        response = await self.idnode_load({'enum': 1, 'class': 'profile'})
        for profile in response.get('entries', []):
            if profile['val'] == "htsp":
                node = htsp_stream_profile_template.copy()
                node['uuid'] = profile['key']
                await self.idnode_save(node)

    async def configure_default_recorder_profile(self, pre_padding_mins=2, post_padding_mins=5, retention_policy="forever"):
        retention_days, removal_days = retention_policy_to_tvh_days(retention_policy)
        recordings_root = (os.environ.get("TVH_RECORDINGS_PATH") or "/recordings").strip() or "/recordings"
        if self.local_conn:
            try:
                os.makedirs(recordings_root, exist_ok=True)
            except OSError as exc:
                logger.warning("Failed to create recordings root '%s': %s", recordings_root, exc)
        response = await self.idnode_load({'enum': 1, 'class': 'dvrconfig'})
        for profile in response.get('entries', []):
            if profile['val'] == "(Default profile)":
                node = default_recorder_profile_template.copy()
                node["storage"] = recordings_root
                node["pre-extra-time"] = int(pre_padding_mins or 0)
                node["post-extra-time"] = int(post_padding_mins or 0)
                node["retention-days"] = int(retention_days)
                node["removal-days"] = int(removal_days)
                node['uuid'] = profile['key']
                await self.idnode_save(node)

    async def ensure_user_recorder_profile(
        self,
        username,
        profile_name,
        pathname,
        pre_padding_mins=2,
        post_padding_mins=5,
        retention_policy="forever",
    ):
        try:
            retention_days, removal_days = retention_policy_to_tvh_days(retention_policy)
            recordings_root = (os.environ.get("TVH_RECORDINGS_PATH") or "/recordings").strip() or "/recordings"
            scoped_pathname = _build_user_scoped_pathname(username, pathname)
            if self.local_conn:
                try:
                    os.makedirs(recordings_root, exist_ok=True)
                except OSError as exc:
                    logger.warning(
                        "Failed to create recordings root '%s' for user '%s': %s",
                        recordings_root,
                        username,
                        exc,
                    )
            response = await self.idnode_load({'enum': 1, 'class': 'dvrconfig'})
            profile_uuid = None
            for profile in response.get('entries', []):
                if profile.get('val') == profile_name:
                    profile_uuid = profile.get('key')
                    break

            node = default_recorder_profile_template.copy()
            node["name"] = profile_name
            node["comment"] = f"TIC user profile for {username}"
            node["pathname"] = scoped_pathname
            node["storage"] = recordings_root
            node["pre-extra-time"] = int(pre_padding_mins or 0)
            node["post-extra-time"] = int(post_padding_mins or 0)
            node["retention-days"] = int(retention_days)
            node["removal-days"] = int(removal_days)

            if not profile_uuid:
                url = f"{self.api_url}/{api_dvr_config_create}"
                payload = {"conf": json.dumps(node)}
                response = await self.__post(url, payload=payload)
                try:
                    created = json.loads(response)
                    profile_uuid = created.get("uuid")
                except json.JSONDecodeError:
                    profile_uuid = None

            if profile_uuid:
                node["uuid"] = profile_uuid
                await self.idnode_save(node)
                return profile_name
        except Exception as exc:
            logger.warning("Failed to ensure user recorder profile '%s': %s", profile_name, exc)
        return ""

    async def configure_timeshift(self):
        url = f"{self.api_url}/{api_timeshift_config_save}"
        node = default_timeshift_config_template.copy()
        await self.__post(url, payload={"node": json.dumps(node)})

    async def list_premade_scanfiles(self, adapter_type):
        url = f"{self.api_url}/{api_list_scanfile}"
        post_data = {"type": adapter_type}
        response = await self.__post(url, payload=post_data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        return json_list["entries"]

    async def list_all_network_builders(self):
        url = f"{self.api_url}/{api_view_network_builders}"
        response = await self.__get(url, payload={})
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        return json_list["entries"]

    async def list_cur_networks(self):
        url = f"{self.api_url}/{api_view_networks}"
        post_data = {
            "start":    "0", "limit": "999999999", "sort": "networkname", "dir": "ASC", "groupBy": "false",
            "groupDir": "ASC"
        }
        response = await self.__post(url, payload=post_data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        return json_list["entries"]

    async def create_network(self, name, network_name, max_streams, priority):
        url = f"{self.api_url}/{api_create_network}"
        net_conf = network_template.copy()
        net_conf['networkname'] = name
        net_conf['pnetworkname'] = network_name
        net_conf['max_streams'] = max_streams
        net_conf['priority'] = priority
        post_data = {"class": "iptv_network", "conf": json.dumps(net_conf)}
        # Send request to server
        response = await self.__get(url, payload=post_data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {}
        return json_list.get('uuid')

    async def delete_network(self, net_uuid):
        await self.idnode_delete(net_uuid)

    async def list_all_muxes(self):
        url = f"{self.api_url}/{api_view_muxes}"
        post_data = {"start": "0", "limit": "999999999", "sort": "name", "dir": "ASC"}
        response = await self.__post(url, payload=post_data)
        try:
            json_list = json.loads(response)
        except:
            json_list = {"entries": []}
        return json_list["entries"]

    async def network_mux_create(self, net_uuid):
        url = f"{self.api_url}/{api_create_mux}"
        mux_conf = mux_template.copy()
        data = {"uuid": net_uuid, "conf": json.dumps(mux_conf)}
        response = await self.__post(url, payload=data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {}
        return json_list.get('uuid')

    async def delete_mux(self, mux_uuid):
        url = f"{self.api_url}/{api_idnode_delete}"
        await self.__post(url, payload={"uuid": mux_uuid})

    async def list_all_services(self):
        url = f"{self.api_url}/{api_list_all_services}"
        post_data = {
            "start":    "0", "limit": "999999999", "sort": "svcname", "dir": "ASC", "groupBy": "false",
            "groupDir": "ASC"
        }
        response = await self.__post(url, payload=post_data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        return json_list["entries"]

    async def map_all_services_to_channels(self):
        url = f"{self.api_url}/{api_services_mapper}"
        srv_info = await self.list_all_services()
        services = []
        for service in srv_info:
            services.append(service["uuid"])
        data = {"node": json.dumps({"services": services, "encrypted": False, "merge_same_name": True})}
        response = await self.__post(url, payload=data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {}
        return json_list

    async def run_internal_epg_grabber(self):
        url = f"{self.api_url}/{api_int_epggrab_run}"
        await self.__post(url, payload={'rerun': 1})

    async def list_all_managed_channel_tags(self):
        url = f"{self.api_url}/{api_list_all_channel_tags}"
        post_data = {"limit": "999999999", "sort": "name", "dir": "ASC", "all": 1}
        response = await self.__post(url, payload=post_data)
        return_list = []
        try:
            json_list = json.loads(response)
            for tvh_channel_tag in json_list["entries"]:
                if tvh_channel_tag.get('comment') == channel_tag_comment:
                    return_list.append(tvh_channel_tag)
        except json.JSONDecodeError:
            return_list = []
        return return_list

    async def create_channel_tag(self, tag_name):
        url = f"{self.api_url}/{api_create_channel_tag}"
        channel_conf = channel_tag_template.copy()
        channel_conf["name"] = tag_name
        channel_conf["comment"] = channel_tag_comment
        # Send request to server
        post_data = {"conf": json.dumps(channel_conf)}
        response = await self.__get(url, payload=post_data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {}
        return json_list.get('uuid')

    async def list_all_channels(self):
        url = f"{self.api_url}/{api_list_all_channels}"
        post_data = {"start": "0", "limit": "999999999", "sort": "services", "dir": "ASC", "all": 1}
        response = await self.__post(url, payload=post_data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {"entries": []}
        return json_list["entries"]

    async def create_channel(self, channel_name, channel_number, logo_url):
        url = f"{self.api_url}/{api_create_channel}"
        channel_conf = channel_template.copy()
        channel_conf["name"] = channel_name
        channel_conf["number"] = channel_number
        channel_conf["icon"] = logo_url
        # Send request to server
        post_data = {"conf": json.dumps(channel_conf)}
        response = await self.__get(url, payload=post_data)
        try:
            json_list = json.loads(response)
        except json.JSONDecodeError:
            json_list = {}
        return json_list.get('uuid')

    async def delete_channel(self, chan_uuid):
        url = f"{self.api_url}/{api_idnode_delete}"
        await self.__post(url, payload={"uuid": chan_uuid})

    async def manage_client_user_access(self, enabled, username, password):
        if enabled:
            await self.create_and_configure_client_user(username, password)
        else:
            await self.remove_client_user()


async def get_tvh(config):
    conn = await config.tvh_connection_settings()
    tvh_local = conn.get('tvh_local', False)
    tvh_host = conn.get('tvh_host')
    tvh_port = conn.get('tvh_port')
    tvh_path = conn.get('tvh_path')
    tvh_username = conn.get('tvh_username')
    tvh_password = conn.get('tvh_password')
    return Tvheadend(tvh_host, tvh_port, tvh_path, tvh_username, tvh_password, tvh_local)


async def get_tvh_with_credentials(config, username, password):
    conn = await config.tvh_connection_settings()
    tvh_local = conn.get('tvh_local', False)
    tvh_host = conn.get('tvh_host')
    tvh_port = conn.get('tvh_port')
    tvh_path = conn.get('tvh_path')
    return Tvheadend(tvh_host, tvh_port, tvh_path, username, password, tvh_local)


async def configure_tvh(config):
    settings = config.read_settings()
    tvh_stream_username, tvh_stream_key = await get_tvh_stream_auth(config)
    conn_settings = await config.tvh_connection_settings()
    if conn_settings.get("tvh_local"):
        resolved_base_url = f"http://127.0.0.1:{flask_run_port}"
    else:
        resolved_base_url = settings["settings"].get("app_url") or ""
    async with await get_tvh(config) as tvh:
        # Update Base Config
        await tvh.save_tvh_config(tvh_config)
        # Update Image Cache Config
        await tvh.save_imagecache_config(tvh_imagecache_config)
        # Ensure internal sync user exists
        await ensure_tvh_sync_user(config)
        # Configure EPG Grab Config
        await tvh.save_epggrab_config(epggrab_config)
        # Disable all EPG grabbers
        await tvh.disable_all_epg_grabbers()
        # Enable XMLTV URL grabber
        epg_url = append_stream_key(
            f"{LOCAL_PROXY_HOST_PLACEHOLDER}/tic-web/epg.xml",
            tvh_stream_key,
            tvh_stream_username,
        )
        if resolved_base_url:
            epg_url = epg_url.replace(LOCAL_PROXY_HOST_PLACEHOLDER, resolved_base_url)
        await tvh.enable_xmltv_url_epg_grabber(epg_url)
        # Configure the default stream profile
        await tvh.configure_default_stream_profile()
        # Configure the htsp stream profile
        await tvh.configure_htsp_stream_profile()
        # Configure the default recorder profile
        dvr_settings = settings.get('settings', {}).get('dvr', {})
        pre_padding_mins = dvr_settings.get('pre_padding_mins', 2)
        post_padding_mins = dvr_settings.get('post_padding_mins', 5)
        retention_policy = normalize_retention_policy(dvr_settings.get('retention_policy'))
        await tvh.configure_default_recorder_profile(pre_padding_mins, post_padding_mins, retention_policy)
        # Configure the timeshift settings
        await tvh.configure_timeshift()


async def ensure_tvh_sync_user(config):
    sync_user = await asyncio.to_thread(config.get_tvh_sync_user)
    if not sync_user.get("username") or not sync_user.get("password"):
        return
    try:
        async with await get_tvh(config) as tvh:
            await tvh.upsert_user(
                sync_user.get("username", "tic-admin"),
                sync_user.get("password"),
                is_admin=True,
                enabled=True,
                access_comment=tvh_sync_access_entry_comment,
                password_comment=tvh_sync_password_comment,
            )
        if not sync_user.get("provisioned"):
            sync_user["provisioned"] = True
            await asyncio.to_thread(config.update_tvh_sync_user, sync_user)
    except Exception as exc:
        logger.exception("Failed to provision TVH sync user: %s", exc)
