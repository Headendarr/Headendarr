#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import logging
import socket
import time
import aiohttp
import base64
from urllib.parse import parse_qsl, urlparse, urlunparse
from backend.config import flask_run_port

logger = logging.getLogger("tic.stream_diagnostics")


class StreamProbe:
    def __init__(self, url, bypass_proxies=False, request_host_url=None, preferred_user_agent=None):
        self.url = url
        self.bypass_proxies = bypass_proxies
        self.request_host_url = request_host_url
        self.preferred_user_agent = (preferred_user_agent or "").strip() or None
        self.task_id = None
        self.status = "pending"
        self.report = {
            "url": url,
            "resolved_url": url,
            "final_url": url,
            "proxy_hops_count": 0,
            "proxy_chain": [],
            "dns": {},
            "geo": {},
            "probe": {
                "avg_speed": 0,
                "avg_bitrate": 0,
                "health": "unknown",
                "summary": ""
            },
            "errors": [],
            "logs": []
        }
        self._running = False

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
        return "/tic-hls-proxy/" in path and "/stream/" in path

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
        errors = self.report.get("errors", [])

        if errors:
            probe["health"] = "critical"
            probe["summary"] = f"Test failed: {errors[0]}"
            return

        if speed == 0:
            if bitrate > 50000:
                probe["health"] = "uncertain"
                probe["summary"] = f"Downloaded at {bitrate/1000000:.2f} Mbps, but could not verify playback clock."
            else:
                probe["health"] = "critical"
                probe["summary"] = "No data received. The stream appears to be offline or blocked."
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

    async def run(self):
        self._running = True
        try:
            # Absolute hard limit for the entire diagnostic run (Dead-man's switch)
            async with asyncio.timeout(45):
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

                self.status = "geo"
                await self._fetch_geo()

                self.status = "probing"
                await self._run_hybrid_probe()

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
            ip = await asyncio.wait_for(
                loop.run_in_executor(None, socket.gethostbyname, hostname),
                timeout=10.0
            )
            self.report["dns"] = {"hostname": hostname, "ip": ip}
            self.log(f"Resolved to {ip}")
        except asyncio.TimeoutError:
            self.log("DNS resolution timed out.")
            raise Exception("DNS Timeout")
        except Exception as e:
            self.log(f"DNS failed: {e}")
            raise

    async def _fetch_geo(self):
        ip = self.report["dns"].get("ip")
        if not ip:
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://ip-api.com/json/{ip}", timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "success":
                            self.report["geo"] = {"country": data.get(
                                "country"), "city": data.get("city"), "isp": data.get("isp")}
                            self.log(f"Location: {data.get('city')}, {data.get('country')} ({data.get('isp')})")
        except:
            pass

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
        self.log("Starting hybrid FFmpeg/Python probe (20s wall-clock limit)...")
        default_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        user_agent_candidates = []
        if self.preferred_user_agent:
            user_agent_candidates.append(self.preferred_user_agent)
        if default_user_agent not in user_agent_candidates:
            user_agent_candidates.append(default_user_agent)

        if self._is_streaming_proxy_endpoint(self.url):
            # Stream proxy endpoints may intentionally delay first-byte delivery (e.g. prebuffer),
            # which can make HTTP preflight produce false negatives. Trust FFmpeg probe instead.
            user_agent = user_agent_candidates[0]
            self.log("Skipping HTTP preflight for TIC stream proxy endpoint; validating via FFmpeg probe.")
        else:
            user_agent = None

        # Fast preflight helps catch auth/routing issues before FFmpeg runs.
        last_preflight_error = None
        if user_agent is None:
            for candidate in user_agent_candidates:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            self.url,
                            headers={"User-Agent": candidate},
                            timeout=aiohttp.ClientTimeout(total=None, connect=6, sock_connect=6, sock_read=6),
                        ) as preflight:
                            if preflight.status >= 400:
                                raise Exception(f"Preflight failed with HTTP {preflight.status}")
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
                        "Preflight request failed with configured user-agent candidate: "
                        f"{type(exc).__name__}: {exc}"
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

        if self.preferred_user_agent and user_agent == self.preferred_user_agent:
            self.log("Preflight succeeded using source-configured user-agent.")
        elif self.preferred_user_agent and user_agent != self.preferred_user_agent:
            self.log("Preflight succeeded using fallback browser user-agent.")

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-user_agent", user_agent,
            "-probesize", "500k",
            "-analyzeduration", "500k",
            "-i", self.url,
            "-c", "copy",
            "-f", "mpegts",
            "pipe:1"
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        start_time = time.time()
        total_bytes = 0
        first_pcr, last_pcr, pcr_pid = None, None, None
        buffer = bytearray()

        try:
            # Strictly enforce a 25s window
            while (time.time() - start_time) < 25:
                try:
                    # Read chunk with timeout to ensure loop doesn't block
                    chunk = await asyncio.wait_for(process.stdout.read(65536), timeout=1.0)
                    if not chunk:
                        break

                    now = time.time()
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
                    elapsed = now - start_time
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
            self.log(f"Probe complete. Bitrate: {res['avg_bitrate']/1000000:.2f} Mbps, Speed: {res['avg_speed']:.2f}x")

        except Exception as e:
            self.log(f"Probe error: {e}")
            if process.returncode is None:
                try:
                    process.kill()
                    await process.wait()
                except:
                    pass


_active_probes = {}


async def start_probe(url, bypass_proxies=False, request_host_url=None, preferred_user_agent=None):
    import uuid
    task_id = str(uuid.uuid4())
    probe = StreamProbe(
        url,
        bypass_proxies=bypass_proxies,
        request_host_url=request_host_url,
        preferred_user_agent=preferred_user_agent,
    )
    probe.task_id = task_id
    _active_probes[task_id] = probe
    asyncio.create_task(probe.run())
    return task_id


def get_probe_status(task_id):
    probe = _active_probes.get(task_id)
    return {"status": probe.status, "report": probe.report} if probe else None


def delete_probe(task_id):
    if task_id in _active_probes:
        del _active_probes[task_id]
