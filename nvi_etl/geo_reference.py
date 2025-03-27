import json
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import geopandas as gpd
import pandas as pd
import datetime

from nvi_etl import db_engine, working_dir


WORKING_DIR = working_dir(__file__)


location_map = json.loads(
    (WORKING_DIR / "conf" / "location_map.json").read_text()
)

def pin_location(row):
    return location_map[row["geo_type"]][row["geography"]]


def pull_city_boundary():
    q = text("""
    SELECT *
    FROM nvi.city_boundary;
    """)
    try:
        return gpd.read_postgis(q, db_engine, geom_col="geometry")
    except OperationalError:
        raise NotImplementedError("Run the scripts to load the geography tables in 'aux_geographies'")
    

def pull_council_districts(year):
    """
    Pull the council districts at the year provided.
    """
    start_date = datetime.date(year=year, month=1, day=1)

    q = text("""
    SELECT *
    FROM nvi.detroit_council_districts
    WHERE start_date = :start_date;
    """)

    try:
        return gpd.read_postgis(q, db_engine, params={"start_date": start_date}, geom_col="geometry")
    except OperationalError:
        raise NotImplementedError("Run the scripts to load the geography tables in 'aux_geographies'")


def pull_zones(year):
    """
    Pull the NVI neighborhood_zones at the year provided.
    """
    start_date = datetime.date(year=year, month=1, day=1)

    q = text("""
    SELECT *
    FROM nvi.neighborhood_zones
    WHERE start_date = :start_date;
    """)

    try:
        return gpd.read_postgis(q, db_engine, params={"start_date": start_date}, geom_col="geometry")
    except OperationalError:
        raise NotImplementedError("Run the scripts to load the geography tables in 'aux_geographies'")


def pull_tracts_to_nvi_crosswalk(tract_year, district_year):
    """
    Pull the translation file between census tracts and NVI zones and 
    Detroit City Council Districts.
    """
    tract_start_date = datetime.date(year=tract_year, month=1, day=1)
    zone_start_date = datetime.date(year=district_year, month=1, day=1)

    q = text("""
    SELECT *
    FROM nvi.tracts_to_nvi_crosswalk
    WHERE zone_start_date = :zone_start_date
    AND tract_start_date = :tract_start_date;
    """)

    try:
        return pd.read_sql(q, db_engine, params={
                "tract_start_date": tract_start_date, 
                "zone_start_date": zone_start_date
        })
    except OperationalError:
        raise NotImplementedError("Run the scripts to load the geography tables in 'aux_geographies'")

