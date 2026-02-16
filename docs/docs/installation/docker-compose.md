---
title: Docker Compose
---

# Docker Compose Installation

Using Docker Compose is the recommended method for installing Headendarr, as it simplifies the deployment and management of the application and its required services.

This guide assumes you have Docker and Docker Compose installed on your system.

## All-in-One (AIO) Mode

This is the preferred setup. It runs the Headendarr web interface and an integrated TVHeadend instance within a single, cohesive Docker container.

### 1. Create a `docker-compose.yml` file

Create a new file named `docker-compose.yml` and add the following content:

```yaml
services:
  tvh-iptv-config:
    image: ghcr.io/headendarr/headendarr-lite:latest
    restart: unless-stopped
    ports:
      - "9985:9985" # Headendarr Web UI
      - "9981:9981" # TVHeadend Web UI
      - "9982:9982" # TVHeadend HTSP
    environment:
      - PUID=1000           # Process user ID
      - PGID=1000           # Process group ID
      - TZ=Pacific/Auckland # Timezone
    volumes:
      - "/path/to/your/config_dir:/config"
      - "/path/to/your/recordings_dir:/recordings"
      - "/path/to/your/timeshift_temp_dir:/timeshift"
```

### 2. Configure Volumes

You **must** change the volume paths to match your system's directory structure:

- `- "/path/to/your/config_dir:/config"`: This is the most important volume. It stores all of your Headendarr configuration, database, and the TVHeadend settings. **Choose a permanent location for this data.**
- `- "/path/to/your/recordings_dir:/recordings"`: This is where any DVR recordings will be saved.
- `- "/path/to/your/timeshift_temp_dir:/timeshift"`: This is where a temprary timeshift recording is held while a stream is being played.

### 3. Start the Container

Open a terminal in the same directory where you saved your `docker-compose.yml` file and run the following command:

```bash
docker-compose up -d
```

Docker will now pull the latest image and start the container in the background.

### 4. Accessing the Application

Once the container is running, you can access the service on the port you exposed for the Headendarr Web UI:

- **Headendarr Web UI**: `http://<your-docker-host-ip>:9985`

:::warning Direct TVHeadend Access
It is **not recommended** to expose the TVHeadend Web UI (`9981`) or HTSP port (`9982`) directly to the internet. These services are best accessed within your local network, or securely remotely via a VPN like [Tailscale](https://tailscale.com/).
:::

## Required Ports

To use all features of the application, ensure the following ports are accessible:

- **9985 (TCP)**: The main web interface for Headendarr where you will manage all your settings.
- **9982 (TCP Optional/Recommended)**: The TVHeadend HTSP (Home TV Streaming Protocol) port. You only need to expose this if you wish to configure TVHeadend HTSP Client to connect and stream channels.
- **9981 (TCP Optional/Not Recommended)**: The web interface and API for the integrated TVHeadend instance. You only need to expose this if you wish to configure a TVHeadend client with HTTP connection.
