#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import ipaddress
import logging
import socket
import time
import aiohttp
import base64
import shutil
from urllib.parse import parse_qsl, urlparse, urlunparse
from backend.config import flask_run_port
from backend.hls_multiplexer import get_header_value
from backend.http_headers import sanitise_headers
from backend.source_media import probe_stream_media_shape

logger = logging.getLogger("tic.stream_diagnostics")


class StreamProbe:
    def __init__(
        self,
        url,
        bypass_proxies=False,
        request_host_url=None,
        preferred_user_agent=None,
        preferred_headers=None,
        probe_window_seconds=12,
        no_data_timeout_seconds=30,
        hard_timeout_seconds=45,
        include_geo_lookup=True,
    ):
        self.url = url
        self.bypass_proxies = bypass_proxies
        self.request_host_url = request_host_url
        self.preferred_user_agent = (preferred_user_agent or "").strip() or None
        self.preferred_headers = sanitise_headers(preferred_headers)
        self.probe_window_seconds = max(5, int(probe_window_seconds or 12))
        self.no_data_timeout_seconds = max(5, int(no_data_timeout_seconds or 30))
        self.hard_timeout_seconds = max(self.probe_window_seconds + 5, int(hard_timeout_seconds or 45))
        self.include_geo_lookup = bool(include_geo_lookup)
        self.task_id = None
        self.status = "pending"
        self.report = {
            "url": url,
            "resolved_url": url,
            "final_url": url,
            "proxy_hops_count": 0,
            "proxy_chain": [],
            "dns": {},
            "connection": {},
            "trace": {"target": None, "protocol": "", "hops": [], "completed": False},
            "geo": {},
            "media": {},
            "probe": {
                "time_to_first_media_seconds": None,
                "avg_speed": 0,
                "avg_bitrate": 0,
                "health": "unknown",
                "summary": "",
            },
            "errors": [],
            "logs": [],
        }
        self._running = False
        self._cancel_requested = False
        self._cancel_reason = ""
        self._ffmpeg_process = None

    def cancel(self, reason="cancelled"):
        self._cancel_requested = True
        self._cancel_reason = str(reason or "cancelled")
        process = self._ffmpeg_process
        if process and process.returncode is None:
            try:
                process.terminate()
            except Exception:
                pass

    def log(self, message):
        timestamp = time.strftime("%H:%M:%S")
        self.report["logs"].append(f"[{timestamp}] {message}")
        logger.info(f"[{self.url}] {message}")

    @staticmethod
    def _is_localhost(hostname):
        if not hostname:
            return False
        host = hostname.lower()
        return host in {"localhost", "127.0.0.1", "::1"}

    @staticmethod
    def _detect_container_ip():
        # Best-effort local interface detection for containerized deployments.
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect(("8.8.8.8", 80))
                return sock.getsockname()[0]
        except Exception:
            try:
                return socket.gethostbyname(socket.gethostname())
            except Exception:
                return None

    @staticmethod
    def _is_tic_internal_path(path: str) -> bool:
        if not path:
            return False
        return "/tic-hls-proxy/" in path or path.startswith("/tic-api/")

    @staticmethod
    def _is_timeout_error(exc: Exception) -> bool:
        if isinstance(exc, asyncio.TimeoutError):
            return True
        return exc.__class__.__name__ in {"SocketTimeoutError", "ServerTimeoutError"}

    @staticmethod
    def _is_streaming_proxy_endpoint(raw_url: str) -> bool:
        path = (urlparse(raw_url).path or "").lower()
        return (
            "/tic-hls-proxy/" in path
            or path.startswith("/tic-api/cso/channel_stream/")
            or path.startswith("/tic-api/cso/channel/")
            or path.startswith("/tic-api/tvh_stream/stream/channel/")
        )

    @staticmethod
    def _build_ffmpeg_headers_arg(headers):
        lines = []
        for key, value in (headers or {}).items():
            lower = str(key or "").strip().lower()
            if lower in {"user-agent", "referer"}:
                continue
            text = str(value or "").strip()
            if not text:
                continue
            lines.append(f"{key}: {text}")
        if not lines:
            return None
        return "\r\n".join(lines) + "\r\n"

    def _normalize_localhost_url(self, raw_url: str, log: bool = False) -> str:
        parsed = urlparse(raw_url)
        if not self._is_localhost(parsed.hostname):
            return raw_url

        host = self._detect_container_ip()
        port = flask_run_port

        if not host:
            return raw_url

        netloc = f"{host}:{port}" if port else host
        normalized = parsed._replace(netloc=netloc)
        new_url = urlunparse(normalized)
        if log:
            self.log(f"Normalised localhost URL to container-reachable URL: {new_url}")
        return new_url

    def _normalize_localhost_proxy_url(self):
        self.url = self._normalize_localhost_url(self.url, log=True)
        self.report["resolved_url"] = self.url

    @staticmethod
    def _socket_family_name(family: int) -> str:
        if family == socket.AF_INET:
            return "ipv4"
        if family == socket.AF_INET6:
            return "ipv6"
        return str(int(family))

    @staticmethod
    def _extract_response_peer(response):
        connection = getattr(response, "connection", None)
        transport = getattr(connection, "transport", None) if connection is not None else None
        if transport is None:
            return None
        peername = transport.get_extra_info("peername")
        if isinstance(peername, tuple) and peername:
            return {"ip": str(peername[0]), "port": int(peername[1]) if len(peername) > 1 else None}
        return None

    def _record_connection_endpoint(self, response):
        final_url = str(getattr(response, "url", "") or "").strip()
        final_parsed = urlparse(final_url) if final_url else urlparse(self.url)
        peer = self._extract_response_peer(response) or {}
        connection_report = {
            "final_url": final_url or self.url,
            "final_hostname": final_parsed.hostname,
            "final_port": final_parsed.port,
            "peer_ip": peer.get("ip"),
            "peer_port": peer.get("port"),
        }
        self.report["connection"] = connection_report
        if connection_report.get("peer_ip"):
            self.log(
                "HTTP preflight connected to "
                f"{connection_report['peer_ip']}:{connection_report.get('peer_port') or '?'} "
                f"for host {connection_report.get('final_hostname') or 'unknown'}"
            )

    def _route_trace_target(self):
        connection = self.report.get("connection") or {}
        peer_ip = str(connection.get("peer_ip") or "").strip()
        if peer_ip:
            return peer_ip
        dns = self.report.get("dns") or {}
        primary_ip = str(dns.get("primary_ip") or "").strip()
        if primary_ip:
            return primary_ip
        return str(dns.get("hostname") or "").strip() or None

    @staticmethod
    def _decode_b64_url_candidate(candidate: str):
        value = (candidate or "").strip()
        if not value:
            return None
        if value.startswith("http://") or value.startswith("https://"):
            return value

        trimmed = value.rsplit(".", 1)[0] if "." in value else value
        padded = trimmed + "=" * (-len(trimmed) % 4)
        decoders = (base64.urlsafe_b64decode, base64.b64decode)
        for decoder in decoders:
            try:
                decoded = decoder(padded).decode("utf-8")
                if decoded.startswith("http://") or decoded.startswith("https://"):
                    return decoded
            except Exception:
                continue
        return None

    def _extract_next_proxy_target(self, raw_url: str):
        parsed = urlparse(raw_url)
        candidates = []

        for segment in reversed(parsed.path.rstrip("/").split("/")):
            if segment:
                candidates.append(segment)

        for _, value in parse_qsl(parsed.query, keep_blank_values=False):
            if value:
                candidates.append(value)

        for candidate in candidates:
            decoded = self._decode_b64_url_candidate(candidate)
            if decoded:
                return self._normalize_localhost_url(decoded)
        return None

    def _build_proxy_chain(self):
        chain = []
        current = self.url
        seen = {current}

        for hop in range(1, 11):
            decoded = self._extract_next_proxy_target(current)
            if not decoded or decoded in seen:
                break
            parsed_current = urlparse(current)
            parsed_next = urlparse(decoded)
            chain.append(
                {
                    "hop": hop,
                    "proxy_url": current,
                    "proxy_hostname": parsed_current.hostname,
                    "target_url": decoded,
                    "target_hostname": parsed_next.hostname,
                }
            )
            self.log(f"Detected proxy hop {hop}: {current} -> {decoded}")
            seen.add(decoded)
            current = decoded

        self.report["proxy_chain"] = chain
        self.report["proxy_hops_count"] = len(chain)
        self.report["final_url"] = current
        if chain:
            self.log(f"Final upstream URL after unwrapping proxies: {current}")

    def _generate_summary(self):
        probe = self.report["probe"]
        speed = probe.get("avg_speed", 0)
        bitrate = probe.get("avg_bitrate", 0)
        startup_seconds = probe.get("time_to_first_media_seconds")
        errors = self.report.get("errors", [])
        has_usable_media = bool(self.report.get("media")) or speed > 0 or bitrate > 50000

        if errors and not has_usable_media:
            probe["health"] = "critical"
            probe["summary"] = f"Test failed: {errors[0]}"
            return

        if speed == 0:
            if bitrate > 50000:
                probe["health"] = "uncertain"
                startup_text = (
                    f" First media data arrived in {startup_seconds:.2f}s." if startup_seconds is not None else ""
                )
                probe["summary"] = (
                    f"Downloaded at {bitrate / 1000000:.2f} Mbps, but could not verify playback clock.{startup_text}"
                )
            else:
                probe["health"] = "critical"
                probe["summary"] = "No data received. The stream appears to be offline or blocked."
            return

        if errors:
            if speed >= 1.05:
                probe["health"] = "good"
                probe["summary"] = (
                    f"Stream delivered media successfully ({speed:.2f}x) despite transient probe errors: {errors[0]}"
                )
            elif speed >= 0.9:
                probe["health"] = "fair"
                probe["summary"] = (
                    f"Stream delivered media with transient probe errors; playback may be unstable ({speed:.2f}x)."
                )
            else:
                probe["health"] = "poor"
                probe["summary"] = (
                    f"Stream delivered media but performance was weak and probe errors occurred ({speed:.2f}x)."
                )
            return

        if speed < 0.9:
            probe["health"] = "poor"
            probe["summary"] = f"Download speed ({speed:.2f}x) is too slow for real-time playback."
        elif speed < 1.05:
            probe["health"] = "fair"
            probe["summary"] = f"Download speed ({speed:.2f}x) is borderline."
        else:
            probe["health"] = "good"
            probe["summary"] = f"Stream is performing well ({speed:.2f}x)."

        if startup_seconds is not None:
            probe["summary"] = f"Startup took {startup_seconds:.2f}s. {probe['summary']}"

    async def run(self):
        self._running = True
        try:
            # Absolute hard limit for the entire diagnostic run (Dead-man's switch)
            async with asyncio.timeout(self.hard_timeout_seconds):
                self._normalize_localhost_proxy_url()
                self._build_proxy_chain()
                if self.bypass_proxies and self.report.get("final_url"):
                    self.url = self.report["final_url"]
                    self.log(f"Bypassing {self.report.get('proxy_hops_count', 0)} proxy hop(s) to test upstream URL.")
                elif self.report.get("proxy_hops_count", 0) > 0:
                    self.log(
                        f"Detected {self.report.get('proxy_hops_count')} proxy hop(s); network route will be resolved against final upstream endpoint."
                    )

                self.log(f"Starting diagnostics for URL: {self.url}")
                self.status = "resolving"
                route_url = self.report.get("final_url") or self.url
                await self._resolve_dns(route_url)

                self.status = "probing"
                await self._run_hybrid_probe()
                if self._cancel_requested:
                    self.status = "cancelled"
                    self.log(f"Diagnostic was cancelled: {self._cancel_reason}")
                    return

                if self.include_geo_lookup:
                    self.status = "geo"
                    await self._fetch_geo()

                self._generate_summary()
                self.status = "finished"
                if self.report.get("errors"):
                    self.log("Diagnostic test completed with errors.")
                else:
                    self.log("Diagnostic test completed successfully.")
        except asyncio.TimeoutError:
            self.status = "finished"
            self.report["errors"].append("Diagnostic timed out (hard limit reached).")
            self.log("Test reached global timeout limit. Returning partial results.")
            self._generate_summary()
        except Exception as e:
            self.status = "error"
            if str(e) not in self.report["errors"]:
                self.report["errors"].append(str(e))
            self.log(f"Test failed: {e}")
        finally:
            self._running = False

    async def _resolve_dns(self, target_url=None):
        self.log("Resolving hostname...")
        hostname = urlparse(target_url or self.url).hostname
        if not hostname:
            raise ValueError("Invalid URL")
        try:
            loop = asyncio.get_running_loop()
            addrinfo = await asyncio.wait_for(
                loop.run_in_executor(None, lambda: socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)),
                timeout=10.0,
            )
            answers = []
            seen = set()
            for family, _socktype, _proto, _canonname, sockaddr in addrinfo:
                address = str(sockaddr[0]).strip() if sockaddr else ""
                if not address or address in seen:
                    continue
                seen.add(address)
                answers.append({"family": self._socket_family_name(family), "address": address})
            primary_ip = next((item["address"] for item in answers if item["family"] == "ipv4"), None)
            if primary_ip is None and answers:
                primary_ip = answers[0]["address"]
            self.report["dns"] = {
                "hostname": hostname,
                "primary_ip": primary_ip,
                "ip": primary_ip,
                "answers": answers,
            }
            if answers:
                answer_summary = ", ".join(f"{item['address']} ({item['family']})" for item in answers[:6])
                extra_count = len(answers) - 6
                if extra_count > 0:
                    answer_summary = f"{answer_summary}, +{extra_count} more"
                self.log(f"DNS answers: {answer_summary}")
            elif primary_ip:
                self.log(f"Resolved to {primary_ip}")
            else:
                self.log("DNS returned no addresses.")
        except asyncio.TimeoutError:
            self.log("DNS resolution timed out.")
            raise Exception("DNS Timeout")
        except Exception as e:
            self.log(f"DNS failed: {e}")
            raise

    async def _fetch_geo(self):
        connection = self.report.get("connection") or {}
        ip = connection.get("peer_ip") or self.report["dns"].get("primary_ip") or self.report["dns"].get("ip")
        if not ip:
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://ip-api.com/json/{ip}", timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "success":
                            self.report["geo"] = {
                                "ip": ip,
                                "country": data.get("country"),
                                "city": data.get("city"),
                                "isp": data.get("isp"),
                            }
                            self.log(f"Location: {data.get('city')}, {data.get('country')} ({data.get('isp')})")
        except:
            pass

    async def _run_route_trace(self):
        target = self._route_trace_target()
        if not target:
            return
        traceroute_binary = shutil.which("traceroute")
        if not traceroute_binary:
            self.log("Traceroute is not available in this environment.")
            return

        parsed_url = urlparse(self.report.get("connection", {}).get("final_url") or self.url)
        target_port = parsed_url.port or (443 if parsed_url.scheme == "https" else 80)
        protocol = "tcp"
        command = [
            traceroute_binary,
            "-n",
            "-T",
            "-p",
            str(int(target_port)),
            "-q",
            "1",
            "-w",
            "1",
            "-m",
            "20",
            target,
        ]
        self.report["trace"] = {"target": target, "protocol": protocol, "hops": [], "completed": False}
        self.log(f"Starting route trace to {target} using TCP/{target_port}.")

        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_data, stderr_data = await asyncio.wait_for(process.communicate(), timeout=18.0)
            if process.returncode not in (0, None):
                stderr_text = (stderr_data or b"").decode("utf-8", errors="replace").strip()
                self.log(f"Traceroute exited with code {process.returncode}: {stderr_text or 'no stderr output'}")
                return
            output = (stdout_data or b"").decode("utf-8", errors="replace")
            hops = []
            for raw_line in output.splitlines():
                line = raw_line.strip()
                if not line or line.lower().startswith("traceroute to "):
                    continue
                if line[0].isdigit():
                    parts = line.split()
                    if not parts:
                        continue
                    try:
                        hop_number = int(parts[0])
                    except ValueError:
                        continue
                    address = ""
                    latency_ms = None
                    for token in parts[1:]:
                        text = str(token).strip()
                        if not text or text == "ms":
                            continue
                        if text == "*":
                            if not address:
                                address = "*"
                            continue
                        if address in {"", "*"}:
                            try:
                                ipaddress.ip_address(text)
                                address = text
                                continue
                            except ValueError:
                                pass
                        try:
                            latency_ms = float(text)
                            break
                        except ValueError:
                            continue
                    hop = {"hop": hop_number, "address": address or "*", "latency_ms": latency_ms}
                    hops.append(hop)
                    if hop["address"] == "*":
                        self.log(f"Route hop {hop_number}: no reply")
                    elif latency_ms is None:
                        self.log(f"Route hop {hop_number}: {hop['address']}")
                    else:
                        self.log(f"Route hop {hop_number}: {hop['address']} {latency_ms:.1f} ms")
            self.report["trace"] = {"target": target, "protocol": protocol, "hops": hops, "completed": True}
            if hops:
                self.log(f"Route trace completed with {len(hops)} hop(s).")
        except asyncio.TimeoutError:
            self.log("Traceroute timed out; returning partial route results if available.")
            if process and process.returncode is None:
                process.kill()
                await process.wait()
        except Exception as exc:
            self.log(f"Traceroute failed: {exc}")
            if process and process.returncode is None:
                process.kill()
                await process.wait()

    def _extract_pcr(self, packet):
        if len(packet) < 188 or packet[0] != 0x47:
            return None
        afc = (packet[3] & 0x30) >> 4
        if afc < 2 or packet[4] == 0 or not (packet[5] & 0x10):
            return None
        try:
            b = packet[6:11]
            return (b[0] << 25) | (b[1] << 17) | (b[2] << 9) | (b[3] << 1) | (b[4] >> 7)
        except:
            return None

    async def _run_hybrid_probe(self):
        self.log(f"Starting hybrid FFmpeg/Python probe ({int(self.probe_window_seconds)}s wall-clock limit)...")
        default_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        base_headers = sanitise_headers(self.preferred_headers)
        configured_header_user_agent = get_header_value(base_headers, "User-Agent")
        user_agent_candidates = []
        if configured_header_user_agent:
            user_agent_candidates.append(configured_header_user_agent)
        if self.preferred_user_agent:
            user_agent_candidates.append(self.preferred_user_agent)
        if default_user_agent not in user_agent_candidates:
            user_agent_candidates.append(default_user_agent)

        if self._is_streaming_proxy_endpoint(self.url):
            # Stream proxy endpoints may intentionally delay first-byte delivery (e.g. prebuffer),
            # which can make HTTP preflight produce false negatives. Trust FFmpeg probe instead.
            user_agent = user_agent_candidates[0]
            preflight_skipped = True
            self.log("Skipping HTTP preflight for TIC stream proxy endpoint; validating via FFmpeg probe.")
        else:
            user_agent = None
            preflight_skipped = False

        # Fast preflight helps catch auth/routing issues before FFmpeg runs.
        last_preflight_error = None
        if user_agent is None:
            for candidate in user_agent_candidates:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            self.url,
                            headers={**base_headers, "User-Agent": candidate},
                            timeout=aiohttp.ClientTimeout(total=None, connect=6, sock_connect=6, sock_read=6),
                        ) as preflight:
                            if preflight.status >= 400:
                                raise Exception(f"Preflight failed with HTTP {preflight.status}")
                            self._record_connection_endpoint(preflight)
                            # For live stream endpoints, do not wait for full body completion.
                            # A successful status + ability to read initial bytes is enough.
                            try:
                                await asyncio.wait_for(preflight.content.read(1), timeout=6.0)
                            except Exception as read_exc:
                                if self._is_streaming_proxy_endpoint(self.url) and self._is_timeout_error(read_exc):
                                    self.log(
                                        "Preflight received successful response from stream proxy endpoint; "
                                        "continuing despite delayed first media byte."
                                    )
                                else:
                                    raise
                    user_agent = candidate
                    break
                except Exception as exc:
                    last_preflight_error = exc
                    self.log(
                        f"Preflight request failed with configured user-agent candidate: {type(exc).__name__}: {exc}"
                    )

        if not user_agent:
            message = (
                f"{type(last_preflight_error).__name__}: {last_preflight_error}"
                if last_preflight_error
                else "Unknown preflight error"
            )
            self.report["errors"].append(message)
            self.log(f"Preflight request failed: {message}")
            return

        if preflight_skipped:
            if self.preferred_user_agent and user_agent == self.preferred_user_agent:
                self.log("Using source-configured user-agent for FFmpeg probe.")
            elif self.preferred_user_agent and user_agent != self.preferred_user_agent:
                self.log("Using fallback browser user-agent for FFmpeg probe.")
        elif self.preferred_user_agent and user_agent == self.preferred_user_agent:
            self.log("Preflight succeeded using source-configured user-agent.")
        elif self.preferred_user_agent and user_agent != self.preferred_user_agent:
            self.log("Preflight succeeded using fallback browser user-agent.")

        route_trace_task = asyncio.create_task(self._run_route_trace())

        media_shape = await probe_stream_media_shape(
            self.url,
            user_agent=user_agent,
            request_headers=base_headers,
            timeout_seconds=8.0,
        )
        if media_shape:
            self.report["media"] = media_shape
            self.log(
                "Media shape detected: "
                f"video={media_shape.get('video_codec') or 'n/a'} "
                f"{int(media_shape.get('width') or 0)}x{int(media_shape.get('height') or 0)} "
                f"{media_shape.get('avg_frame_rate') or 'n/a'} "
                f"pix_fmt={media_shape.get('pixel_format') or 'n/a'} "
                f"audio={media_shape.get('audio_codec') or 'n/a'}"
            )

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-user_agent",
            user_agent,
            "-probesize",
            "500k",
            "-analyzeduration",
            "500k",
            "-i",
            self.url,
            "-c",
            "copy",
            "-f",
            "mpegts",
            "pipe:1",
        ]
        referer_value = get_header_value(base_headers, "Referer")
        extra_headers = self._build_ffmpeg_headers_arg({**base_headers, "User-Agent": user_agent})
        if referer_value:
            insert_idx = cmd.index("-probesize")
            cmd[insert_idx:insert_idx] = ["-referer", referer_value]
        if extra_headers:
            insert_idx = cmd.index("-probesize")
            cmd[insert_idx:insert_idx] = ["-headers", extra_headers]

        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        self._ffmpeg_process = process

        start_time = time.time()
        sample_start_time = None
        total_bytes = 0
        first_pcr, last_pcr, pcr_pid = None, None, None
        buffer = bytearray()

        try:
            # Wait up to no_data_timeout_seconds for first media bytes.
            # Once bytes arrive, run probe_window_seconds of sampling.
            while True:
                if self._cancel_requested:
                    break
                now = time.time()
                if sample_start_time is None:
                    if (now - start_time) >= self.no_data_timeout_seconds:
                        if not self.report["errors"]:
                            self.report["errors"].append(
                                f"No media bytes received within {self.no_data_timeout_seconds}s."
                            )
                        self.log(f"No media bytes received within {self.no_data_timeout_seconds}s; stopping probe.")
                        break
                elif (now - sample_start_time) >= self.probe_window_seconds:
                    break
                try:
                    # Read chunk with timeout to ensure loop doesn't block
                    chunk = await asyncio.wait_for(process.stdout.read(65536), timeout=1.0)
                    if not chunk:
                        break

                    now = time.time()
                    if sample_start_time is None:
                        sample_start_time = now
                        self.report["probe"]["time_to_first_media_seconds"] = sample_start_time - start_time
                        self.log(f"First media bytes received; sampling for {int(self.probe_window_seconds)}s.")
                    total_bytes += len(chunk)
                    buffer.extend(chunk)

                    # Parse TS packets from buffer
                    while len(buffer) >= 188:
                        sync = buffer.find(0x47)
                        if sync == -1:
                            buffer.clear()
                            break
                        if sync > 0:
                            del buffer[:sync]
                        if len(buffer) < 188:
                            break
                        packet = buffer[:188]
                        pcr = self._extract_pcr(packet)
                        if pcr is not None:
                            pid = ((packet[1] & 0x1F) << 8) | packet[2]
                            if pcr_pid is None:
                                pcr_pid = pid
                            if pid == pcr_pid:
                                if first_pcr is None:
                                    first_pcr = pcr
                                last_pcr = pcr
                        del buffer[:188]

                    # Update live stats
                    elapsed = now - sample_start_time if sample_start_time else 0
                    if elapsed > 0:
                        self.report["probe"]["avg_bitrate"] = (total_bytes * 8) / elapsed
                        if first_pcr is not None and last_pcr is not None:
                            dur = (last_pcr - first_pcr) / 90000.0
                            if dur < 0:
                                dur += (2**33) / 90000.0
                            self.report["probe"]["avg_speed"] = dur / elapsed

                except asyncio.TimeoutError:
                    continue

            # Hard cleanup of subprocess
            if process.returncode is None:
                try:
                    process.terminate()
                    # Give it 2s to terminate gracefully, then kill
                    try:
                        await asyncio.wait_for(process.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        self.log("FFmpeg did not terminate gracefully. Killing...")
                        process.kill()
                        await process.wait()
                except:
                    pass
            elif process.returncode != 0:
                if self._cancel_requested and process.returncode in (-15, 255):
                    return
                try:
                    stderr_output = await process.stderr.read()
                    stderr_text = stderr_output.decode("utf-8", errors="replace").strip()
                except Exception:
                    stderr_text = ""
                msg = f"FFmpeg exited with code {process.returncode}"
                if stderr_text:
                    msg = f"{msg}: {stderr_text.splitlines()[-1]}"
                self.report["errors"].append(msg)
                self.log(msg)

            if total_bytes == 0 and not self.report["errors"]:
                self.report["errors"].append("No media bytes received from stream.")
                self.log("No media bytes received from stream.")

            res = self.report["probe"]
            startup_seconds = res.get("time_to_first_media_seconds")
            startup_log = f", Startup: {startup_seconds:.2f}s" if startup_seconds is not None else ""
            self.log(
                f"Probe complete. Bitrate: {res['avg_bitrate'] / 1000000:.2f} Mbps, "
                f"Speed: {res['avg_speed']:.2f}x{startup_log}"
            )

        except Exception as e:
            self.log(f"Probe error: {e}")
            if process.returncode is None:
                try:
                    process.kill()
                    await process.wait()
                except:
                    pass
        finally:
            if "route_trace_task" in locals():
                try:
                    await route_trace_task
                except Exception:
                    pass
            self._ffmpeg_process = None


_active_probes = {}


async def start_probe(
    url,
    bypass_proxies=False,
    request_host_url=None,
    preferred_user_agent=None,
    preferred_headers=None,
    on_complete=None,
):
    import uuid

    task_id = str(uuid.uuid4())
    probe = StreamProbe(
        url,
        bypass_proxies=bypass_proxies,
        request_host_url=request_host_url,
        preferred_user_agent=preferred_user_agent,
        preferred_headers=preferred_headers,
    )
    probe.task_id = task_id
    _active_probes[task_id] = probe

    async def _run_and_complete():
        try:
            await probe.run()
        finally:
            if on_complete is not None:
                try:
                    await on_complete(probe)
                except Exception as exc:
                    logger.exception("Failed to execute stream diagnostics completion hook: %s", exc)

    asyncio.create_task(_run_and_complete())
    return task_id


def get_probe_status(task_id):
    probe = _active_probes.get(task_id)
    return {"status": probe.status, "report": probe.report} if probe else None


def delete_probe(task_id):
    if task_id in _active_probes:
        del _active_probes[task_id]
