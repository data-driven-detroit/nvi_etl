"""SQLAlchemy engine factory."""

import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, Engine, text

from nvi_etl.config import db_url


def get_engine(db_name: str | None = None) -> Engine:
    """Create a SQLAlchemy engine for the given database."""
    return create_engine(db_url(db_name))


def read_sql_file(path: Path, engine: Engine, params: dict | None = None) -> pd.DataFrame:
    """Read a SQL file and execute it, returning a DataFrame."""
    q = text(path.read_text())
    return pd.read_sql(q, engine, params=params)
