---
title: Reverse Proxy and Client IP Hardening
---

# Reverse Proxy and Client IP Hardening

When Headendarr is behind a reverse proxy, client IP addresses can be forwarded using headers such as `X-Forwarded-For`.

By default, Headendarr does **not** trust these headers from arbitrary clients.

## Trusted Proxy Header Settings

| Variable                  | Description                                                                  | Default | Example                                 |
| ------------------------- | ---------------------------------------------------------------------------- | ------- | --------------------------------------- |
| `TIC_TRUST_PROXY_HEADERS` | Trust forwarded IP headers only when requests come from trusted proxy CIDRs. | `false` | `true`                                  |
| `TIC_TRUSTED_PROXY_CIDRS` | Comma-separated proxy CIDRs allowed to provide forwarded headers.            | _empty_ | `172.18.0.0/16,10.0.0.0/8,127.0.0.1/32` |
| `TIC_AUTH_COOKIE_SECURE`  | Marks auth/OIDC cookies as Secure (HTTPS-only in browsers).                  | `false` | `true`                                  |

:::warning
Do not enable `TIC_TRUST_PROXY_HEADERS=true` without setting `TIC_TRUSTED_PROXY_CIDRS`.
If you do, clients can spoof audit/login IP addresses by sending forged forwarding headers.
:::

## Docker Compose Example

```yaml
environment:
  - TIC_TRUST_PROXY_HEADERS=true
  - TIC_TRUSTED_PROXY_CIDRS=172.18.0.0/16,10.0.0.0/8,127.0.0.1/32
  - TIC_AUTH_COOKIE_SECURE=true
```

Use `TIC_AUTH_COOKIE_SECURE=true` when browser access is HTTPS (for example behind a TLS reverse proxy).
Keep it `false` for direct plain-HTTP LAN deployments.

## Reverse Proxy Examples

Use these as starting points. Keep your proxy and Headendarr on private network paths.

### Nginx

```nginx
location / {
  proxy_pass http://headendarr:9985;
  proxy_set_header Host $host;
  proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  proxy_set_header X-Forwarded-Proto $scheme;
  proxy_set_header X-Real-IP $remote_addr;
}
```

### Caddy

```caddy
headendarr.example.com {
  reverse_proxy headendarr:9985 {
    header_up X-Forwarded-For {remote_host}
    header_up X-Forwarded-Proto {scheme}
    header_up X-Real-IP {remote_host}
  }
}
```

### Traefik (Docker labels)

```yaml
labels:
  - traefik.enable=true
  - traefik.http.routers.headendarr.rule=Host(`headendarr.example.com`)
  - traefik.http.services.headendarr.loadbalancer.server.port=9985
```

Traefik forwards client IP information automatically when configured as the entrypoint.

## Auth Rate Limiting (In-memory)

Headendarr can throttle repeated authentication requests to reduce brute-force login attempts.

This limiter is kept in process memory:

- It applies immediately while the app is running.
- Counters reset when the container/app restarts.

| Variable                                   | Description                                                            | Default | Example |
| ------------------------------------------ | ---------------------------------------------------------------------- | ------- | ------- |
| `TIC_AUTH_RATE_LIMIT_ENABLED`              | Enables auth endpoint rate limiting.                                   | `true`  | `true`  |
| `TIC_AUTH_LOGIN_IP_WINDOW_SECONDS`         | Sliding window length for failed login attempts per IP.                | `600`   | `600`   |
| `TIC_AUTH_LOGIN_IP_MAX_ATTEMPTS`           | Maximum failed login attempts per IP within the IP window.             | `10`    | `10`    |
| `TIC_AUTH_LOGIN_USER_WINDOW_SECONDS`       | Sliding window length for failed login attempts per username.          | `600`   | `600`   |
| `TIC_AUTH_LOGIN_USER_MAX_ATTEMPTS`         | Maximum failed login attempts per username within the username window. | `5`     | `5`     |
| `TIC_AUTH_LOGIN_COOLDOWN_BASE_SECONDS`     | Base cooldown after repeated login failures.                           | `2`     | `2`     |
| `TIC_AUTH_LOGIN_COOLDOWN_MAX_SECONDS`      | Maximum cooldown cap after repeated failures.                          | `60`    | `60`    |
| `TIC_AUTH_OIDC_START_IP_WINDOW_SECONDS`    | Sliding window length for OIDC start requests per IP.                  | `600`   | `600`   |
| `TIC_AUTH_OIDC_START_IP_MAX_ATTEMPTS`      | Maximum OIDC start requests per IP in the start window.                | `60`    | `60`    |
| `TIC_AUTH_OIDC_CALLBACK_IP_WINDOW_SECONDS` | Sliding window length for OIDC callback requests per IP.               | `600`   | `600`   |
| `TIC_AUTH_OIDC_CALLBACK_IP_MAX_ATTEMPTS`   | Maximum OIDC callback requests per IP in the callback window.          | `60`    | `60`    |

### What These Settings Do

- `TIC_AUTH_LOGIN_IP_*`:
  - Limits failed local-login attempts from the same IP address.
  - Default: up to `10` failed attempts per `600` seconds (10 minutes).

- `TIC_AUTH_LOGIN_USER_*`:
  - Limits failed local-login attempts against the same username from the same source IP.
  - Default: up to `5` failed attempts per `600` seconds (10 minutes).

- `TIC_AUTH_LOGIN_COOLDOWN_*`:
  - Adds temporary lockout backoff after repeated failures.
  - Backoff increases with continued failures and is capped by `TIC_AUTH_LOGIN_COOLDOWN_MAX_SECONDS`.

- `TIC_AUTH_OIDC_START_IP_*` and `TIC_AUTH_OIDC_CALLBACK_IP_*`:
  - Limit repeated OIDC start/callback requests per source IP.

### Recommended Starting Values (Home Lab)

If you are unsure, keep the defaults shown above.

If your users frequently share a single public IP (for example through CGNAT or VPN), consider slightly increasing IP thresholds while keeping username thresholds strict.

### How Throttling Appears

When a limit is exceeded, Headendarr returns:

- HTTP status `429 Too Many Requests`
- a `Retry-After` header with the wait time in seconds
