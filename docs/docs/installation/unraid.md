---
title: Unraid
---

# Unraid Installation

Installing Headendarr on Unraid is straightforward using the Community Applications plugin.

This guide assumes you have the **Community Applications** plugin installed on your Unraid server.

## Installing from Community Applications

1.  Navigate to the **Apps** tab in your Unraid web interface.
2.  In the search box, type `Headendarr` and press Enter.
3.  Locate the official `headendarr` application in the search results and click the **Install** button.

![Unraid app search results showing Headendarr install button](/img/screenshots/installation-unraid-search-app-store.png)

## Template Configuration

Unraid will present the Docker container template. Adjust the paths and ports to your environment before applying.

| Parameter            | Description                                                                                              |
| -------------------- | -------------------------------------------------------------------------------------------------------- |
| **Repository**       | `ghcr.io/headendarr/headendarr:latest` (stable) or `ghcr.io/headendarr/headendarr:staging` (pre-release) |
| **Network Type**     | `bridge` (recommended default)                                                                           |
| **Port: 9985 (TCP)** | Headendarr Web UI                                                                                        |
| **Port: 9981 (TCP)** | TVHeadend Web UI                                                                                         |
| **Port: 9982 (TCP)** | TVHeadend HTSP                                                                                           |
| **Path: /config**    | Required app data path (for example `/mnt/user/appdata/headendarr`)                                      |

### Advanced Template Options

In the Unraid template form, enable **Show more settings...** to reveal advanced options.

Advanced options include:

- **Path: `/recordings`**: Set this to a location on your array (for example `/mnt/user/media/dvr`).
- **Path: `/timeshift`**: Set this to fast temporary storage such as your cache pool or memory-backed storage (for example `/dev/shm`). Headendarr uses this for timeshift data and temporary VOD caching, including VOD 24/7 channel cache warm and handoff.
- **Path: `/tmp/cache`**: **(Recommended)** Set this to memory-backed storage (for example `/dev/shm/headendarr`). This is a temporary storage area used by the Channel Stream Orchestrator (CSO) for local segmented ingest and output handoff. Using RAM-based storage here significantly improves performance and avoids unnecessary disk wear. It requires approximately 50MB of space per concurrent VOD stream.
- **Variables**: `TZ`, `PUID`, `PGID`.

You can keep defaults for a first install, but most users should customise `/recordings` and `/timeshift`.

After configuring values, click **Apply** to start the container.

:::info Image Tag Selection
Use `ghcr.io/headendarr/headendarr:latest` for stable builds.
Use `ghcr.io/headendarr/headendarr:staging` if you want pre-release builds for early testing.
:::

## Accessing the Application

Once the container is running, you can access the Headendarr Web UI by clicking the container's icon and selecting **WebUI**. The URL will be `http://<your-unraid-ip>:9985`.

Initial login credentials are:

- **Username**: `admin`
- **Password**: `admin`

:::warning Direct TVHeadend Access
It is **not recommended** to expose the TVHeadend Web UI (`9981`) or HTSP port (`9982`) directly to the internet. These services are best accessed within your local network, or securely remotely via a VPN like [Tailscale](https://tailscale.com/).
:::

## Using an External PostgreSQL Database

By default, Headendarr runs an embedded PostgreSQL database inside the container. If you want to run Headendarr against an external PostgreSQL server, you can configure it by adding custom environment variables to the Unraid template.

This will also automatically skip launching the integrated PostgreSQL service within the container.

### 1. Database Preparation

Please follow the instructions in the [Docker Compose Database Setup](./docker-compose.md#1-database-setup) section to create the connection role/user and database on your external PostgreSQL server.

### 2. Adding Variables in the Unraid Template

To configure the external connection:

1. On the Unraid **Docker** tab, click on the **Headendarr** container icon and select **Edit**.
2. Scroll to the bottom of the template form and click **Add another Path, Port, Variable, Label or Device**.
3. Add the following **Variables**:
   - **POSTGRES_HOST**: (Required) The IP address or hostname of your external PostgreSQL server (e.g. `192.168.1.50`). _Adding this variable is the trigger that enables external database mode._
   - **POSTGRES_PORT**: (Optional) The port of your PostgreSQL server (e.g. `5432`). Defaults to `5432`.
   - **POSTGRES_DB**: (Optional) The database name you created for Headendarr (e.g. `headendarr`). Defaults to `tic`.
   - **POSTGRES_USER**: (Optional) The database user (e.g. `headendarr`). Defaults to `tic`.
   - **POSTGRES_PASSWORD**: (Optional) The password for your database user. Defaults to `tic`.

4. Click **Apply** to restart the container with the new configuration.
