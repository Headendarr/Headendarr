Headendarr
===========================

![Headendarr](https://github.com/Headendarr/Headendarr/raw/master/logo.png)

<a href='https://ko-fi.com/I2I21F8E1' target='_blank'><img height='26' style='border:0px;height:26px;' src='https://cdn.ko-fi.com/cdn/kofi1.png?v=2' border='0' alt='Buy Me a Coffee at ko-fi.com' /></a>

[![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/Headendarr/Headendarr?color=009dc7&label=latest%20release&logo=github&logoColor=%23403d3d&style=flat-square)](https://github.com/Headendarr/Headendarr/releases)
[![GitHub issues](https://img.shields.io/github/issues-raw/Headendarr/Headendarr?color=009dc7&logo=github&logoColor=%23403d3d&style=flat-square)](https://github.com/Headendarr/Headendarr/issues?q=is%3Aopen+is%3Aissue)
[![GitHub closed issues](https://img.shields.io/github/issues-closed-raw/Headendarr/Headendarr?color=009dc7&logo=github&logoColor=%23403d3d&style=flat-square)](https://github.com/Headendarr/Headendarr/issues?q=is%3Aissue+is%3Aclosed)
[![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/Headendarr/Headendarr?color=009dc7&logo=github&logoColor=%23403d3d&style=flat-square)](https://github.com/Headendarr/Headendarr/pulls?q=is%3Aopen+is%3Apr)
[![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed-raw/Headendarr/Headendarr?color=009dc7&logo=github&logoColor=%23403d3d&style=flat-square)](https://github.com/Headendarr/Headendarr/pulls?q=is%3Apr+is%3Aclosed)

[![Docker Stars](https://img.shields.io/docker/stars/josh5/headendarr?color=009dc7&logo=docker&logoColor=%23403d3d&style=for-the-badge)](https://hub.docker.com/r/josh5/headendarr)
[![Docker Pulls](https://img.shields.io/docker/pulls/josh5/headendarr?color=009dc7&logo=docker&logoColor=%23403d3d&style=for-the-badge)](https://hub.docker.com/r/josh5/headendarr)
[![Docker Image Size (tag)](https://img.shields.io/docker/image-size/josh5/headendarr/latest?color=009dc7&label=docker%20image%20size&logo=docker&logoColor=%23403d3d&style=for-the-badge)](https://hub.docker.com/r/josh5/headendarr)



![GitHub Workflow Status (with branch)](https://img.shields.io/github/actions/workflow/status/Headendarr/Headendarr/build_docker_ci.yml?branch=master&logo=github&logoColor=403d3d&style=flat-square)

[![GitHub license](https://img.shields.io/github/license/Headendarr/Headendarr?color=009dc7&style=flat-square)]()
---

Headendarr is a simple wrapper around TVHeadend with the singular goal of making it simple to configure TVHeadend for IPTV playlist.

TVHeadend is an extremely powerful TV streaming server and recorder with excellent support for IPTV sources. However, for most people, setting this up can be difficult and time-consuming.
The goal of this project is to wrap around TVHeadend and, using its API, configure most of the server for you.

### Project Status

This project is currently in beta. I am publishing builds, but the application is very new and could be full of bugs, inefficent processes, or visual blemishes. That being said, if you would like to contribute to this project, feel free to provide a PR.

I will probably not be acknowledging any issue reports or testing. But feel free to also reach out on [Discord](https://support-api.streamingtech.co.nz/discord) if you would like to contribute any suggestions there.

### Table Of Contents

[Dependencies](#dependencies)

[Install and Run](#install-and-run)

[License](#license)


## Dependencies

 - NodeJS ([Install](https://nodejs.org/en/download)).
 - Python 3.x ([Install](https://www.python.org/downloads/)).
 - Various Python requirements listed in 'requirements.txt' in the project root.
 - A TVHeadend server
   - Only TVHeadend v4.3+ is tested, though 4.2.8 may work fine.

## Install and Run

- [Run from source](./docs/run-from-source.md)
- [Run with Docker Compose](./docs/run-with-docker-compose.md)


## Development

### Run from source with docker compose:

From the project root run:
```
mkdir -p ./dev_env/config
docker compose -f ./docker/docker-compose.dev-aio.yml up --build
```

This will create a directory within this project root called `./dev_env` which contains all configuration and cache data.

### Run from source with a Python venv

To setup a development environment, first setup a [Run from source](./docs/run-from-source.md) setup.

Then run the project with debugging tools enabled by using the script
```
./devops/run_local_dev_env.sh
```

### Updating packages
Activate the venv and, from inside the Python venv, run the command:
```
python3 -m pip install -r ./requirements.txt -r ./requirements-dev.txt
```
This will install all the current dev dependencies and tools.

Now run the pip-audit check command. This will print any packages that need to be updated.
```
pip-audit -r ./requirements.txt -r ./requirements-dev.txt
```
This will give you a printout of what is out of date. From there you can select which packages you wish to upgrade.

Once you have upgraded the packages, run this command to upgrade the frozen requirements.
```
pip-compile ./requirements.in --upgrade
```


## License

This projected is licensed under the [Apache 2.0 Licence](./LICENSE). 

Copyright (C) Josh Sunnex - All Rights Reserved.

