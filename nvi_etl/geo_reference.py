from sqlalchemy import text
import geopandas as gpd
import datetime
from nvi_etl import db_engine


def pull_city_boundary():
    q = text("""
    SELECT *
    FROM nvi.detroit_city_boundary;
    """)

    return gpd.read_postgis(q, db_engine)
    

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

    return gpd.read_postgis(q, db_engine, params={"start_date": start_date})


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

    return gpd.read_postgis(q, db_engine, params={"start_date": start_date})


def pull_cw_tracts_districts(tract_year, district_year):
    """
    Pull the NVI neighborhood_zones at the year provided.
    """
    tract_start_date = datetime.date(year=tract_year, month=1, day=1)
    district_start_date = datetime.date(year=district_year, month=1, day=1)

    q = text("""
    SELECT *
    FROM nvi.neighborhood_zones
    WHERE district_start_date = :district_start_date;
    AND tract_start_date = :tract_start_date
    """)

    return gpd.read_postgis(q, db_engine, params={
            "tract_start_date": tract_start_date, 
            "district_start_date": district_start_date
    })


def pull_cw_tracts_zones(tract_year, zone_year):
    """
    Pull the NVI neighborhood_zones at the year provided.
    """
    tract_start_date = datetime.date(year=tract_year, month=1, day=1)
    zone_start_date = datetime.date(year=zone_year, month=1, day=1)

    q = text("""
    SELECT *
    FROM nvi.neighborhood_zones
    WHERE tract_start_date = :tract_start_date
    AND zone_start_date = :zone_start_date
    """)

    return gpd.read_postgis(
        q, db_engine, params={
            "tract_start_date": tract_start_date, 
            "zone_start_date": zone_start_date
        }
    )

