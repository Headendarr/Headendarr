# Run from source

> [!WARNING]  
> This is not the recommended way (or even an activly supported way) for running Headendarr. Please consider using Docker.

## Clone project

Clone this project somewhere
```
git clone https://github.com/Josh5/Headendarr.git
cd Headendarr
```

## Run the build scripts

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and ensure Python 3.13 is available.

1) Run the setup script. This creates a `.venv` using Python 3.13, syncs the exact dependencies in `uv.lock`, and builds
the frontend. Re-run this script whenever you pull updates from GitHub.
```
./devops/setup_local_dev_env.sh
```
2) Run the project.
```
# Create a directory for your config files and export it to HOME_DIR
export HOME_DIR="${PWD}/dev_env/config/"
mkdir -p "${HOME_DIR}"

# Migrate database
uv run --frozen alembic upgrade head

# Run app
uv run --frozen python ./run.py
```

> [!NOTE]  
> These above commands will create a directory within this project root called `./dev_env` which contains all configuration and cache data.
> If you want this config path to be somewhere else, set `HOME_DIR` to something else. You will need to ensure you export this before you run `./run.py` each time.

> [!IMPORTANT]  
> If you are running it like this, you will need to configure all TVH stuff yourself.

## Update project

Pull updates
```
git pull origin master
```

Rebuild
```
./devops/setup_local_dev_env.sh
./devops/run_local_dev_env.sh
```
