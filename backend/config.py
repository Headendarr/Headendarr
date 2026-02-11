import asyncio
import base64
import json
import os
import secrets
import subprocess
from urllib.parse import quote_plus

import aiofiles
import yaml
from mergedeep import merge

from backend.security import generate_stream_key

def get_home_dir():
    home_dir = os.environ.get("HOME_DIR")
    if home_dir is None:
        home_dir = os.path.expanduser("~")
    return home_dir


async def is_tvh_process_running_locally():
    process_name = "tvheadend"
    try:
        process = await asyncio.create_subprocess_exec(
            "pgrep",
            "-x",
            process_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            return True
        else:
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def is_tvh_process_running_locally_sync():
    process_name = "tvheadend"
    try:
        result = subprocess.run(
            ["pgrep", "-x", process_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode == 0:
            return True
        else:
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


async def get_admin_file(directory):
    if os.path.exists(directory) and os.listdir(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                async with aiofiles.open(file_path, "r") as file:
                    try:
                        contents = await file.read()
                        data = json.loads(contents)
                        if data.get("username") == "admin":
                            return file_path, data
                    except (json.JSONDecodeError, IOError) as e:
                        print(f"Error processing file {file_path}: {e}")
    return None, None


async def update_accesscontrol_files():
    accesscontrol_path = os.path.join(get_home_dir(), ".tvheadend", "accesscontrol")
    file_path, data = await get_admin_file(accesscontrol_path)
    if data:
        data["prefix"] = "0.0.0.0/0,::/0"
        async with aiofiles.open(file_path, "w") as outfile:
            await outfile.write(json.dumps(data, indent=4))


async def get_local_tvh_proc_admin_password():
    passwd_path = os.path.join(get_home_dir(), ".tvheadend", "passwd")
    file_path, data = await get_admin_file(passwd_path)
    if data:
        encoded_password = data.get("password2")
        try:
            decoded_password = base64.b64decode(encoded_password).decode("utf-8")
            parts = decoded_password.split("-")
            return parts[2]
        except Exception as e:
            print(f"Error decoding password: {e}")
    return None


def write_yaml(file, data):
    if not os.path.exists(os.path.dirname(file)):
        os.makedirs(os.path.dirname(file))
    with open(file, "w") as outfile:
        yaml.dump(data, outfile, default_flow_style=False)


def read_yaml(file):
    if not os.path.exists(file):
        return {}
    with open(file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def update_yaml(file, new_data):
    if not os.path.exists(os.path.dirname(file)):
        os.makedirs(os.path.dirname(file))
    data = read_yaml(file)
    merge(data, new_data)
    with open(file, "w") as outfile:
        yaml.dump(data, outfile, default_flow_style=False)


def recursive_dict_update(defaults, updates):
    for key, value in updates.items():
        if isinstance(value, dict) and key in defaults:
            recursive_dict_update(defaults[key], value)
        else:
            defaults[key] = value
    return defaults


class Config:
    runtime_key = ""

    def __init__(self, **kwargs):
        # Set default directories
        self.config_path = os.path.join(get_home_dir(), ".tvh_iptv_config")
        self.config_file = os.path.join(self.config_path, "settings.yml")
        self.tvh_sync_user_file = os.path.join(self.config_path, "tvh_sync_user.json")
        self.tvh_stream_user_file = os.path.join(
            self.config_path, "tvh_stream_user.json"
        )
        self.instance_id_file = os.path.join(self.config_path, "instance_id.json")
        # Set default settings
        self.settings = None
        self.tvh_local = is_tvh_process_running_locally_sync()
        self.default_settings = {
            "settings": {
                "first_run": True,
                "tvheadend": {
                    "host": "",
                    "port": "9981",
                    "path": "/",
                    "username": "",
                    "password": "",
                },
                "app_url": None,
                "route_playlists_through_tvh": True,
                "audit_log_retention_days": 7,
                "user_agents": [
                    {
                        "name": "VLC",
                        "value": "VLC/3.0.23 LibVLC/3.0.23",
                    },
                    {
                        "name": "Chrome",
                        "value": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3",
                    },
                    {
                        "name": "TiviMate",
                        "value": "TiviMate/5.1.6 (Android 12)",
                    },
                ],
                "admin_password": "admin",
                "enable_stream_buffer": True,
                "default_ffmpeg_pipe_args": "-hide_banner -loglevel error "
                "-probesize 10M -analyzeduration 0 -fpsprobesize 0 "
                "-i [URL] -c copy -metadata service_name=[SERVICE_NAME] "
                "-f mpegts pipe:1",
                "dvr": {
                    "pre_padding_mins": 2,
                    "post_padding_mins": 5,
                },
                "ui_settings": {
                    "enable_channel_health_highlight": True,
                },
                "epgs": {
                    "enable_tmdb_metadata": False,
                    "tmdb_api_key": "",
                    "enable_google_image_search_metadata": False,
                },
            }
        }

    def create_default_settings_yaml(self):
        self.write_settings_yaml(self.default_settings)

    def write_settings_yaml(self, data):
        write_yaml(self.config_file, data)

    def read_config_yaml(self):
        if not os.path.exists(self.config_file):
            self.create_default_settings_yaml()
        return read_yaml(self.config_file)

    def read_settings(self):
        yaml_settings = {}
        if self.settings is None:
            yaml_settings = self.read_config_yaml()
        self.settings = recursive_dict_update(self.default_settings, yaml_settings)
        return self.settings

    def ensure_tvh_sync_user(self):
        if os.path.exists(self.tvh_sync_user_file):
            return
        if not os.path.exists(os.path.dirname(self.tvh_sync_user_file)):
            os.makedirs(os.path.dirname(self.tvh_sync_user_file))
        sync_user = {
            "username": "tic-admin",
            "password": secrets.token_urlsafe(18),
            "provisioned": False,
        }
        with open(self.tvh_sync_user_file, "w") as f:
            json.dump(sync_user, f, indent=2)

    def ensure_instance_id(self):
        if os.path.exists(self.instance_id_file):
            try:
                with open(self.instance_id_file, "r") as f:
                    data = json.load(f)
                instance_id = data.get("instance_id")
                if instance_id:
                    return instance_id
            except Exception:
                pass
        if not os.path.exists(os.path.dirname(self.instance_id_file)):
            os.makedirs(os.path.dirname(self.instance_id_file))
        # Short, stable instance id for internal proxy routing.
        instance_id = secrets.token_urlsafe(8)[:10]
        with open(self.instance_id_file, "w") as f:
            json.dump({"instance_id": instance_id}, f, indent=2)
        return instance_id

    def get_tvh_sync_user(self):
        self.ensure_tvh_sync_user()
        try:
            with open(self.tvh_sync_user_file, "r") as f:
                return json.load(f)
        except Exception:
            return {"username": "tic-admin", "password": "", "provisioned": False}

    def update_tvh_sync_user(self, data):
        if not os.path.exists(os.path.dirname(self.tvh_sync_user_file)):
            os.makedirs(os.path.dirname(self.tvh_sync_user_file))
        with open(self.tvh_sync_user_file, "w") as f:
            json.dump(data, f, indent=2)

    def ensure_tvh_stream_user(self):
        if os.path.exists(self.tvh_stream_user_file):
            return
        if not os.path.exists(os.path.dirname(self.tvh_stream_user_file)):
            os.makedirs(os.path.dirname(self.tvh_stream_user_file))
        stream_user = {
            "username": f"tic-tvh-{secrets.token_urlsafe(6)}",
            "stream_key": generate_stream_key(),
        }
        with open(self.tvh_stream_user_file, "w") as f:
            json.dump(stream_user, f, indent=2)

    async def get_tvh_stream_user(self):
        await asyncio.to_thread(self.ensure_tvh_stream_user)
        try:
            return await asyncio.to_thread(self._read_tvh_stream_user)
        except Exception:
            return {"username": "", "stream_key": ""}

    def _read_tvh_stream_user(self):
        with open(self.tvh_stream_user_file, "r") as f:
            return json.load(f)

    def save_settings(self):
        if self.settings is None:
            self.create_default_settings_yaml()
        self.write_settings_yaml(self.settings)

    def update_settings(self, updated_settings):
        if self.settings is None:
            self.read_settings()
        self.settings = recursive_dict_update(self.default_settings, updated_settings)

    async def tvh_connection_settings(self):
        settings = await asyncio.to_thread(self.read_settings)
        sync_user = await asyncio.to_thread(self.get_tvh_sync_user)
        if await is_tvh_process_running_locally():
            # Note: Host can be localhost here because the app will publish to TVH from the backend
            tvh_host = "127.0.0.1"
            tvh_port = "9981"
            tvh_path = "/tic-tvh"
            if sync_user.get("provisioned"):
                tvh_username = sync_user.get("username", "tic-admin")
                tvh_password = sync_user.get("password")
            else:
                tvh_username = "admin"
                tvh_password = await get_local_tvh_proc_admin_password()
            return {
                "tvh_local": True,
                "tvh_host": tvh_host,
                "tvh_port": tvh_port,
                "tvh_path": tvh_path,
                "tvh_username": tvh_username,
                "tvh_password": tvh_password,
            }
        if sync_user.get("provisioned") and sync_user.get("password"):
            tvh_username = sync_user.get("username", "tic-admin")
            tvh_password = sync_user.get("password")
        else:
            tvh_username = settings["settings"]["tvheadend"]["username"]
            tvh_password = settings["settings"]["tvheadend"]["password"]
        return {
            "tvh_local": False,
            "tvh_host": settings["settings"]["tvheadend"]["host"],
            "tvh_port": settings["settings"]["tvheadend"]["port"],
            "tvh_path": settings["settings"]["tvheadend"]["path"],
            "tvh_username": tvh_username,
            "tvh_password": tvh_password,
        }


frontend_dir = os.path.join(
    os.path.dirname(os.path.abspath(os.path.dirname(__file__))), "frontend"
)

enable_app_debugging = False
if os.environ.get("ENABLE_APP_DEBUGGING", "false").lower() == "true":
    enable_app_debugging = True

enable_sqlalchemy_debugging = False
if os.environ.get("ENABLE_SQLALCHEMY_DEBUGGING", "false").lower() == "true":
    enable_sqlalchemy_debugging = True

flask_run_host = os.environ.get("FLASK_RUN_HOST", "0.0.0.0")
flask_run_port = int(os.environ.get("FLASK_RUN_PORT", "9985"))

app_basedir = os.path.abspath(os.path.dirname(__file__))
config_path = os.path.join(get_home_dir(), ".tvh_iptv_config")
if not os.path.exists(config_path):
    os.makedirs(config_path)

# Configure Postgres DB
sqlalchemy_database_path = os.path.join(config_path, "db.sqlite3")
postgres_host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
postgres_port = os.environ.get("POSTGRES_PORT", "5432")
postgres_db = os.environ.get("POSTGRES_DB", "tic")
postgres_user = os.environ.get("POSTGRES_USER", "tic")
postgres_password = os.environ.get("POSTGRES_PASSWORD", "tic")
postgres_password_escaped = quote_plus(postgres_password)

sqlalchemy_database_uri = f"postgresql+psycopg://{postgres_user}:{postgres_password_escaped}@{postgres_host}:{postgres_port}/{postgres_db}"
sqlalchemy_database_async_uri = f"postgresql+asyncpg://{postgres_user}:{postgres_password_escaped}@{postgres_host}:{postgres_port}/{postgres_db}"
sqlalchemy_track_modifications = False

# Configure scheduler
scheduler_api_enabled = True

# Set up the App SECRET_KEY
# SECRET_KEY = config('SECRET_KEY'  , default='S#perS3crEt_007')
secret_key = os.getenv("SECRET_KEY", "S#perS3crEt_007")

# Assets Management
assets_root = os.getenv("ASSETS_ROOT", os.path.join(frontend_dir, "dist", "spa"))
