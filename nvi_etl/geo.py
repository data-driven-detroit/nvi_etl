"""Shared geographic data utilities.

All functions require an explicit engine parameter -- no module-level
database connections.
"""

import json
import datetime
from functools import lru_cache

from sqlalchemy import Engine, text
from sqlalchemy.exc import OperationalError
import geopandas as gpd
import pandas as pd

from nvi_etl.config import CONF_DIR


def _load_location_map():
    """Lazily load the location map from conf."""
    return json.loads((CONF_DIR / "location_map.json").read_text())


def pin_location(row, location_map=None):
    """Map a row's geo_type + geography to a location_id."""
    if location_map is None:
        location_map = _load_location_map()
    return location_map[row["geo_type"]][row["geography"]]


def pull_city_boundary(engine: Engine):
    q = text("SELECT * FROM nvi.city_boundary;")
    try:
        return gpd.read_postgis(q, engine, geom_col="geometry")
    except OperationalError as e:
        raise NotImplementedError(
            f"{e}: Run geography loading tasks first"
        )


def pull_council_districts(engine: Engine, year: int):
    """Pull the council districts at the year provided."""
    start_date = datetime.date(year=year, month=1, day=1)

    q = text("""
    SELECT *
    FROM nvi.detroit_council_districts
    WHERE start_date = :start_date;
    """)

    try:
        return gpd.read_postgis(
            q, engine, params={"start_date": start_date}, geom_col="geometry"
        )
    except OperationalError:
        raise NotImplementedError(
            "Run geography loading tasks first"
        )


def pull_cdo_boundaries(engine: Engine, as_of: datetime.date):
    """Pull the CDO boundaries that were active on ``as_of``.

    NOTE: callers currently pass the run date (``datetime.date.today()``), so
    survey results are aggregated to whichever CDO boundaries are active when
    the ETL runs, not the ones in effect when the survey was fielded.
    """
    q = text("""
    SELECT *
    FROM nvi.cdo_boundaries
    WHERE start_date <= :as_of
      AND end_date >= :as_of;
    """)

    try:
        return gpd.read_postgis(
            q, engine, params={"as_of": as_of}, geom_col="geometry"
        )
    except OperationalError:
        raise NotImplementedError(
            "Run geography loading tasks first"
        )


def pull_zones(engine: Engine, year: int):
    """Pull the NVI neighborhood_zones at the year provided."""
    start_date = datetime.date(year=year, month=1, day=1)

    q = text("""
    SELECT *
    FROM nvi.neighborhood_zones
    WHERE start_date = :start_date;
    """)

    try:
        return gpd.read_postgis(
            q, engine, params={"start_date": start_date}, geom_col="geometry"
        )
    except OperationalError:
        raise NotImplementedError(
            "Run geography loading tasks first"
        )


def pull_tracts_to_nvi_crosswalk(engine: Engine, tract_year: int, district_year: int):
    """Pull the translation between census tracts and NVI zones / districts."""
    tract_start_date = datetime.date(year=tract_year, month=1, day=1)
    zone_start_date = datetime.date(year=district_year, month=1, day=1)

    q = text("""
    SELECT *
    FROM nvi.tracts_to_nvi_crosswalk
    WHERE zone_start_date = :zone_start_date
    AND tract_start_date = :tract_start_date;
    """)

    try:
        return pd.read_sql(
            q,
            engine,
            params={
                "tract_start_date": tract_start_date,
                "zone_start_date": zone_start_date,
            },
        )
    except OperationalError:
        raise NotImplementedError(
            "Run geography loading tasks first"
        )
