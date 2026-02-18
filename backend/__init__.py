#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import logging
import time
from importlib import import_module
from logging.config import dictConfig

import quart_flask_patch
from quart import Quart

from backend import config
from backend.api import tasks

dictConfig({
    'version':    1,
    'formatters': {
        'default': {
            'format': '%(asctime)s:%(levelname)s:%(name)s: - %(message)s',
        }
    },
    'loggers':    {
        'quart.app': {
            'level': 'ERROR',
        },
        'apscheduler.executors.default': {
            'level': 'WARNING',
        },
        'apscheduler.scheduler': {
            'level': 'WARNING',
        },
    },
    'handlers':   {
        'wsgi': {
            'class':     'logging.StreamHandler',
            'stream':    'ext://sys.stderr',
            'formatter': 'default'
        }
    },
    'root':       {
        'level':    'INFO',
        'handlers': ['wsgi']
    }
})


# Custom logging filter that ignores log messages for a specific endpoints
class IgnoreLoggingRoutesFilter(logging.Filter):
    def filter(self, record):
        if "/tic-api/get-background-tasks" in record.getMessage():
            return False
        return True


def init_db(app):
    from backend.models import db
    app.config["SQLALCHEMY_DATABASE_URI"] = config.sqlalchemy_database_uri
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = config.sqlalchemy_track_modifications
    # Increase SQLite timeout to reduce 'database is locked' errors under concurrent access
    if config.sqlalchemy_database_uri.startswith("sqlite"):
        app.config.setdefault("SQLALCHEMY_ENGINE_OPTIONS", {})
        engine_opts = app.config["SQLALCHEMY_ENGINE_OPTIONS"]
        connect_args = engine_opts.get("connect_args", {})
        # Only set timeout if not already user-defined
        connect_args.setdefault("timeout", 30)  # seconds
        engine_opts["connect_args"] = connect_args
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = engine_opts
    db.init_app(app)

    # Enable WAL + relaxed synchronous for better concurrent write characteristics on SQLite
    if config.sqlalchemy_database_uri.startswith("sqlite"):
        try:
            from sqlalchemy import text
            with app.app_context():
                db.session.execute(text("PRAGMA journal_mode=WAL"))
                db.session.execute(text("PRAGMA synchronous=NORMAL"))
                db.session.commit()
        except Exception:
            # Ignore if already configured
            pass

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db.session.remove()

    return db


def register_blueprints(app):
    module = import_module('backend.api.routes')
    import_module('backend.api.routes_auth')
    import_module('backend.api.routes_users')
    import_module('backend.api.routes_playlists')
    import_module('backend.api.routes_epgs')
    import_module('backend.api.routes_channels')
    import_module('backend.api.routes_dvr')
    import_module('backend.api.routes_guide')
    import_module('backend.api.routes_audit')
    import_module('backend.api.routes_dashboard')
    import_module('backend.api.routes_playlist_proxy')
    import_module('backend.api.routes_hls_proxy')
    import_module('backend.api.routes_xc')
    import_module('backend.api.routes_diagnostics')
    app.register_blueprint(module.blueprint)


def create_app():
    # Fetch app config
    app_config = config.Config()
    app_config.runtime_key = int(time.time())
    app_config.ensure_tvh_stream_user()
    # Create app
    app = Quart(__name__, instance_relative_config=True)
    app.config["SECRET_KEY"] = config.secret_key
    app.config["SCHEDULER_API_ENABLED"] = config.scheduler_api_enabled
    app.config["APP_CONFIG"] = app_config
    app.config["ASSETS_ROOT"] = config.assets_root

    # Init the DB connection
    db = init_db(app)

    # Register the route blueprints
    register_blueprints(app)

    access_logger = logging.getLogger('hypercorn.access')
    app.logger.setLevel(logging.INFO)
    access_logger.setLevel(logging.INFO)
    if config.enable_app_debugging:
        app.logger.setLevel(logging.DEBUG)
        access_logger.setLevel(logging.DEBUG)
    access_logger.addFilter(IgnoreLoggingRoutesFilter())

    return app
