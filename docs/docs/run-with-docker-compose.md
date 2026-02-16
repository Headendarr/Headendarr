# Docker Compose

Follow these instructions to configure a docker-compose.yml for your system.

> __Note__
>
> These instructions assume that you have docker and docker-compose installed for your system.
>
> Depending on how you have installed this, the commands to execute docker compose may vary.


## PREPARE DIRECTORIES:

> __Warning__
>
> These commands are meant to be run as your user. Do not run them as root.
>
> If you do run these commands as root, you may need to manually fix the permissions and ownership after.

Create a directory for your service:
```shell
sudo mkdir -p /data/containers/tvh-iptv
sudo chown -R $(id -u):$(id -g) /data/containers/tvh-iptv
```

If you modify the path `/data/containers/tvh-iptv`, ensure you also modify the path in your docker-compose.yml file below.


Create a Docker Compose file `/data/containers/tvh-iptv/docker-compose.yml`.

Populate this file with the contents of one of these Docker Compose templates:
- [AIO Stack with Headendarr & TVH in a single container (Recommended)](./compose-files/docker-compose.aio.yml).
- [Side-cart Stack with Headendarr & TVH in separate containers (Requires that you do some initial setup for TVH)](./compose-files/docker-compose.side-tvh.yml).
- [AIO Stack with multiple HLS Proxy side-carts](./compose-files/docker-compose.aio-with-proxies.yml).

## EXECUTE:

Navigate to your compose location and execute it.
```shell
cd /data/containers/tvh-iptv
sudo docker-compose up -d --force-recreate --pull
```

After container executes successfully, navigate to your docker host URL in your browser on port 9985 and click connect.
`http://<host-ip>:9985/`

## Timezone Notes

- Set `TZ` in your compose file for container-local logs/runtime behavior (example: `TZ=Pacific/Auckland`).
- Headendarr persists application timestamps in UTC in the database.
- Headendarr API returns timestamps in UTC (`...Z`) and the frontend renders them in local time for the user/device.
- Keep host clock and container clock in sync (NTP) to avoid skew.
