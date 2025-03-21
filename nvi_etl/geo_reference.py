from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import geopandas as gpd
import datetime
from nvi_etl import db_engine


def pull_city_boundary():
    q = text("""
    SELECT *
    FROM nvi.detroit_city_boundary;
    """)
    try:
        return gpd.read_postgis(q, db_engine)
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
        return gpd.read_postgis(q, db_engine, params={"start_date": start_date})
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
        return gpd.read_postgis(q, db_engine, params={"start_date": start_date})
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
    WHERE zone_start_date = :zone_start_date;
    AND tract_start_date = :tract_start_date
    """)

    try:
        return gpd.read_postgis(q, db_engine, params={
                "tract_start_date": tract_start_date, 
                "district_start_date": zone_start_date
        })
    except OperationalError:
        raise NotImplementedError("Run the scripts to load the geography tables in 'aux_geographies'")

