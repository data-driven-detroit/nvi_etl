"""Environment-based configuration loaded from .env file."""

import json
import logging
import logging.config
import os
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

load_dotenv()

PACKAGE_DIR = Path(__file__).resolve().parent
BASE_DIR = PACKAGE_DIR.parent
CONF_DIR = PACKAGE_DIR / "conf"
SQL_DIR = PACKAGE_DIR / "sql"

DUA_FOLDER = Path(os.environ.get("NVI_DUA_FOLDER", "/mnt/q"))
VAULT_FOLDER = Path(os.environ.get("NVI_VAULT_FOLDER", "/mnt/v"))


def db_url(db_name: str | None = None) -> str:
    """Build a SQLAlchemy connection URL for the given database."""
    user = os.environ["NVI_DB_USER"]
    password = quote(os.environ.get("NVI_DB_PASSWORD", ""), safe="")
    host = os.environ.get("NVI_DB_HOST", "192.168.1.11")
    port = os.environ.get("NVI_DB_PORT", "5432")
    name = db_name or os.environ.get("NVI_DB_NAME", "nvi_test")

    if password:
        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"
    return f"postgresql+psycopg://{user}@{host}:{port}/{name}"


def setup_logging():
    """Configure logging from logging_config.json."""
    config_path = BASE_DIR / "logging_config.json"

    with open(config_path) as f:
        logging_config = json.load(f)

    logging_config["handlers"]["file"]["filename"] = str(
        BASE_DIR / "logs" / "nvi_etl.log"
    )

    logging.config.dictConfig(logging_config)

    return logging.getLogger("nvi_etl")
