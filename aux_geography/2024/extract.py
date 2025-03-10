from pathlib import Path
import pandas as pd
from sqlalchemy import text

from nvi_etl import db_engine


WORKING_DIR = Path(__file__).resolve().parent


def extract_council_districts(logger):
    logger.info("No extraction required.")


def extract_neighborhood_zones(logger):
    logger.info("No extraction required.")


def extract_2010_tracts_to_2026_council_districts(logger):
    logger.info("Extracting 2026 council districts to 2010 tracts crosswalk.")

    cw_q = text(
        """
    with michigan_tracts as (
        select *, '14000US' || geoid10 as long_geoid
        from shp.tiger_census_2010_tract_mi
        where statefp10 = '26'
    )
    select tracts.long_geoid as tract_geoid, 
           cds.district_number,
           DATE '2010-01-01' as tract_start_date,
           DATE '2019-12-31' as tract_end_date,
           DATE '2026-01-01' as district_start_date,
           DATE '2036-01-01' as district_end_date
    from nvi.detroit_council_districts cds
    join michigan_tracts tracts
        on st_within(st_centroid(st_transform(tracts.geom, 2898)), cds.geometry)
    where cds.start_date = DATE '2026-01-01'
    order by cds.district_number;
    """
    )

    df = pd.read_sql(cw_q, db_engine)

    df.to_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2026_cw.csv",
        index=False,
    )


def extract_2020_tracts_to_2026_council_districts(logger):
    logger.info("Extracting 2026 council districts to 2020 tracts crosswalk.")
    cw_q = text(
        """
    with michigan_tracts as (
        select *, '14000US' || geoid as long_geoid
        from shp.tiger_census_2020_tract_mi
        where statefp = '26'
    )
    select tracts.long_geoid as tract_geoid, 
           cds.district_number,
           DATE '2020-01-01' as tract_start_date,
           DATE '2029-12-31' as tract_end_date,
           DATE '2026-01-01' as district_start_date,
           DATE '2036-01-01' as district_end_date
    from nvi.detroit_council_districts cds
    join michigan_tracts tracts
        on st_within(st_centroid(st_transform(tracts.geom, 2898)), cds.geometry)
    where cds.start_date = DATE '2026-01-01'
    order by cds.district_number;
    """
    )

    df = pd.read_sql(cw_q, db_engine)

    df.to_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2026_cw.csv",
        index=False,
    )


def extract_2010_tracts_to_2026_nvi_zones(logger):
    logger.info("Extracting 2026 NVI zones to 2010 tracts crosswalk.")

    cw_q = text(
        """
    with michigan_tracts as (
        select *, '14000US' || geoid10 as long_geoid
        from shp.tiger_census_2010_tract_mi
        where statefp10 = '26'
    )
    select tracts.long_geoid as tract_geoid,
           nzs.zone_id,
           nzs.district_number,
           DATE '2010-01-01' as tract_start_date,
           DATE '2019-12-31' as tract_end_date,
           DATE '2026-01-01' as district_start_date,
           DATE '2036-01-01' as district_end_date
    from nvi.neighborhood_zones nzs
    join michigan_tracts tracts
        on st_within(st_centroid(st_transform(tracts.geom, 2898)), nzs.geometry)
    where nzs.start_date = DATE '2026-01-01'
    order by nzs.district_number;
    """
    )

    df = pd.read_sql(cw_q, db_engine)

    df.to_csv(
        WORKING_DIR / "output" / "tracts_zones_2010_2026_cw.csv",
        index=False,
    )


def extract_2020_tracts_to_2026_nvi_zones(logger):
    logger.info("Extracting 2026 NVI zones to 2020 tracts crosswalk.")

    cw_q = text(
        """
    with michigan_tracts as (
        select *, '14000US' || geoid as long_geoid
        from shp.tiger_census_2020_tract_mi
        where statefp = '26'
    )
    select tracts.long_geoid as tract_geoid,
           nzs.zone_id,
           nzs.district_number,
           DATE '2020-01-01' as tract_start_date,
           DATE '2029-12-31' as tract_end_date,
           DATE '2026-01-01' as district_start_date,
           DATE '2036-01-01' as district_end_date
    from nvi.neighborhood_zones nzs
    join michigan_tracts tracts
        on st_within(st_centroid(st_transform(tracts.geom, 2898)), nzs.geometry)
    where nzs.start_date = DATE '2026-01-01'
    order by nzs.district_number;
    """
    )

    df = pd.read_sql(cw_q, db_engine)

    df.to_csv(
        WORKING_DIR / "output" / "tracts_zones_2020_2026_cw.csv",
        index=False,
    )
