---
title: HLS Proxies
---

# HLS Proxy Management

Headendarr features a sophisticated HLS (HTTP Live Streaming) proxy system designed to enhance stream stability, unlock geographic restrictions, and provide granular control over how your IPTV sources are consumed.

## What is an HLS Proxy?

An HLS proxy acts as an intermediary between your IPTV provider and your streaming client (like TVHeadend or Plex). Instead of your client connecting directly to the provider's server, it connects to Headendarr. Headendarr then fetches the stream segments, potentially modifies them, and serves them to your client.

This is useful for:

- **Hiding your home IP**: The provider only sees the IP of the proxy.
- **Bypassing Geo-blocks**: By routing traffic through a proxy in a different region.
- **Improving Stability**: Through segment caching and automated retries.
- **Compatibility**: Remuxing incompatible stream formats into standardised MPEG-TS.

## Built-in HLS Proxy

Headendarr comes with a high-performance HLS proxy built directly into the application.

### Key Features

- **Authentication**: Secure your proxy endpoints using the same **Streaming Keys** used for client connectivity.
- **Connection Sharing**: Maximise your connection limits. Headendarr can share a single upstream connection among multiple local clients, serving them from a unified local cache.
- **Segment Caching**: Automatically cache HLS segments (`.ts` files) to reduce latency and provide smoother playback, especially over unstable connections.
- **Intelligent Pre-caching**: Headendarr doesn't just cache what the client asks for; it proactively "looks ahead" in the playlist. While serving the current segment, the proxy automatically pre-downloads the next few segments it predicts the client will need. By the time your client requests the next chunk, it's already sitting in Headendarr's local cache, resulting in near-instant delivery and significantly smoother playback.

* **Stream Remuxing**: Utilises **FFmpeg** to remux streams into a stable MPEG-TS format on-the-fly. This is particularly useful for sources that provide raw HLS that some clients struggle to decode natively.
* **Audit Logging**: Every connection through the proxy is logged, allowing you to monitor bandwidth usage and stream health in real-time.

---

## External HLS Proxy Support

While Headendarr has a powerful built-in proxy, you can also utilise **external HLS proxies**. This is configured on a per-source basis and is ideal for advanced workflows like **Proxy Chaining** through VPN-backed containers.

### Why Chain Proxies?

Proxy chaining (`Client -> Headendarr -> External Proxy -> Provider`) is an essential technique for complex network environments.

- **Network Isolation (e.g., Tailscale)**: If you run Headendarr behind a VPN or overlay network like Tailscale, you may only expose a single IP address for the application. Your remote clients might not have direct access to other external HLS proxies or VPN containers running on different ports or IPs in your local network. By chaining, Headendarr acts as a gateway; your clients only ever need to talk to Headendarr, which then handles the internal routing to your various regional HLS proxies.
- **Performance vs. Latency**: While adding a second proxy layer introduces a tiny amount of initial start-up latency (typically just a fraction of a second), the overall experience is often much smoother. This is because both the internal and external proxies utilise the **Intelligent Pre-caching** mentioned above, ensuring segments are pulled into your local network well before your client actually needs them.

For a detailed walkthrough on setting up external proxies with VPN routing, see our [External HLS Proxy via VPN Guide](../guides/external-hls-proxy).
