---
title: External HLS Proxy via VPN
---

# External HLS Proxy with VPN Routing

This guide explains how to set up an external HLS proxy routed through a VPN using **Gluetun**. This advanced configuration is ideal for bypassing geographic restrictions (geo-blocking) on specific IPTV sources or improving stream stability by routing traffic through a specific region.

## Overview

By using an external proxy, you can isolate VPN traffic to just your IPTV streams. Headendarr then "chains" to this proxy:
`Client -> Headendarr -> External Proxy (VPN) -> IPTV Provider`

## Prerequisites

- A working Docker and Docker Compose environment.
- A VPN provider compatible with [Gluetun](https://github.com/qdm12/gluetun) (e.g., NordVPN, Mullvad, ProtonVPN, etc.).
- The [josh5/hls-proxy](https://github.com/Josh5/HLS-Proxy) Docker image.

## Docker Compose Configuration

Create a directory for your proxy setup and save the following `docker-compose.yml`. This example includes the environment variables inline with detailed comments explaining their purpose.

Review the available environment variable options for your provider in the [Glutun Wiki](https://github.com/qdm12/gluetun-wiki/tree/main/setup/providers)

```yaml
services:
  gluetun:
    image: qmcgaw/gluetun:latest
    container_name: vpn-australia
    restart: unless-stopped
    cap_add:
      - NET_ADMIN
    ports:
      - "9972:9972" # HLS Proxy Port
    dns:
      - 1.1.1.1
      - 8.8.4.4
    environment:
      # --- VPN PROVIDER CONFIG ---
      - VPN_SERVICE_PROVIDER=nordvpn # Your VPN provider name
      - VPN_TYPE=wireguard # wireguard or openvpn

      # For Wireguard
      - WIREGUARD_PRIVATE_KEY= # Your private key
      - WIREGUARD_ADDRESSES= # Your assigned VPN address (e.g. 10.0.0.2/32)

      # For OpenVPN (Alternative)
      # - OPENVPN_USER=
      # - OPENVPN_PASSWORD=

      # --- SERVER SELECTION ---
      - SERVER_COUNTRIES=United States # Route traffic through United States
      - SERVER_CITIES=Los Angeles # Optional: Target a specific city
      - STREAM_ONLY=on # Prefer servers optimized for streaming

      # --- DNS & HEALTH ---
      - DNS_ADDRESS=1.1.1.1
      - DOT=off # DNS over TLS
      - HEALTH_VPN_DURATION_INITIAL=20s
      - HEALTH_SUCCESS_WAIT_DURATION=30s

  proxy:
    image: josh5/hls-proxy:latest
    container_name: hls-proxy-au
    restart: unless-stopped
    network_mode: "service:gluetun" # Routes all proxy traffic through Gluetun
    environment:
      # --- HLS PROXY CONFIG ---
      - HLS_PROXY_LOG_LEVEL=1 # 0=Debug, 1=Info, 2=Warning, 3=Error
      - HLS_PROXY_PORT=9972 # Must match the port exposed in gluetun

      # The IP address of your Docker host.
      # This is used to build the URLs for the HLS segments.
      - HLS_PROXY_HOST_IP=192.168.1.12
```

## Multi-Region Proxy Chaining

You can run multiple external HLS proxies simultaneously to support different regional requirements for different IPTV sources.

### Multi-Region Docker Compose Example

In this example, we set up two separate HLS proxies, each routed through a different VPN location using Gluetun. Headendarr can then be configured to use these proxies for specific sources.

```yaml
services:
  # VPN Client for UK
  vpn-uk:
    image: qmcgaw/gluetun
    container_name: vpn-uk
    cap_add:
      - NET_ADMIN
    environment:
      - VPN_SERVICE_PROVIDER=nordvpn
      - VPN_TYPE=wireguard
      - WIREGUARD_PRIVATE_KEY=your_key
      - SERVER_COUNTRIES=United Kingdom
    restart: always

  # HLS Proxy routed through UK VPN
  proxy-uk:
    image: josh5/hls-proxy
    container_name: proxy-uk
    network_mode: "container:vpn-uk"
    restart: always

  # VPN Client for USA
  vpn-us:
    image: qmcgaw/gluetun
    container_name: vpn-us
    cap_add:
      - NET_ADMIN
    environment:
      - VPN_SERVICE_PROVIDER=nordvpn
      - VPN_TYPE=wireguard
      - WIREGUARD_PRIVATE_KEY=your_key
      - SERVER_COUNTRIES=United States
    restart: always

  # HLS Proxy routed through USA VPN
  proxy-us:
    image: josh5/hls-proxy
    container_name: proxy-us
    network_mode: "container:vpn-us"
    restart: always

  # Headendarr (Main App)
  headendarr:
    image: josh5/headendarr:latest
    container_name: headendarr
    ports:
      - 9985:9985
    volumes:
      - ./config:/config
    restart: always
```

## Configuring Headendarr

Once your proxy stack is running, you need to tell Headendarr to use it for specific IPTV sources.

1.  Open the **Headendarr Web UI**.
2.  Navigate to the **Sources** page.
3.  Add or edit a source (M3U or Xtream Codes).
4.  Locate the **HLS Proxy** field and enter the full URL of your new external proxy:
    `http://192.168.1.12:9972`
5.  Save the source and perform an **Update**.

Headendarr will now automatically rewrite all stream URLs from this source to route through your VPN-backed proxy.

:::tip Proxy Chaining
For complex network environments, proxy chaining offers a robust way to manage multiple regional sources through a single entry point. Review the [Benefits of Proxy Chaining](#benefits-of-proxy-chaining) below to understand how this simplifies remote access and enhances playback smoothness.
:::

## Benefits of Proxy Chaining

Proxy chaining (`Client -> Headendarr -> External Proxy -> Provider`) might seem complex, but it provides significant advantages for stability and network management.

### Network Isolation and Simplicity (e.g., Tailscale)

If you run Headendarr behind a private overlay network like **Tailscale**, you typically only expose the Headendarr application IP and port (`9985`) to your remote clients. In this scenario, your clients may not have direct network access to other containers or external proxies running in your local network (which might be on different IPs or behind regional VPN containers).

By chaining your sources through Headendarr, you simplify your setup:

- Your clients only ever need to be able to reach **Headendarr**.
- Headendarr handles the "internal" routing to your various regional HLS proxies within your home network.

### Intelligent Pre-caching and Smoothness

While chaining adds a second proxy layer, the impact on start-up latency is negligible—typically only a fraction of a second. This minor trade-off is more than offset by the performance gains from **Intelligent Pre-caching**.

Both the internal and standalone `josh5/hls-proxy` utilise a "look-ahead" mechanism:

1. When a client requests the first segment of a stream, the proxy serves it immediately.
2. Simultaneously, the proxy identifies the next several segments in the HLS playlist.
3. It proactively pre-downloads and caches these future segments in the background.

By the time your client is ready to request the next segment, it is already cached locally, resulting in an exceptionally smooth playback experience that can handle network jitter much better than a direct connection.
