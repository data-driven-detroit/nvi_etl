from pathlib import Path
import pandas as pd
from sqlalchemy import text

from nvi_etl import db_engine


WORKING_DIR = Path(__file__).resolve().parent


def extract_council_districts(logger):
    pass


def extract_neighborhood_zones(logger):
    pass


def extract_2010_tracts_to_2014_council_districts(logger):
    logger.info("Extracting 2014 council districts to 2010 tracts crosswalk.")

    cw_q = text(
        """
    with michigan_tracts as (
        select *
        from tiger.tracts
        where state = '26'
        and year = 2010
    )
    select tracts.long_geoid as tract_geoid, 
           cds.district_number,
           DATE '2010-01-01' as tract_start_date,
           DATE '2019-12-31' as tract_end_date,
           DATE '2014-01-01' as district_start_date,
           DATE '2025-12-31' as district_end_date
    from nvi.detroit_council_districts cds
    join michigan_tracts tracts
        on st_within(st_centroid(st_transform(tracts.geometry, 2898)), cds.geometry)
    where cds.start_date = DATE '2014-01-01'
    order by cds.district_number;
    """
    )

    df = pd.read_sql(cw_q, db_engine)

    df.to_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2014_cw.csv",
        index=False,
    )


def extract_2020_tracts_to_2014_council_districts(logger):
    logger.info("Extracting 2014 council districts to 2010 tracts crosswalk.")

    cw_q = text(
        """
    with michigan_tracts as (
        select *
        from tiger.tracts
        where state = '26'
        and year = 2020
    )
    select tracts.long_geoid as tract_geoid, 
           cds.district_number,
           DATE '2020-01-01' as tract_start_date,
           DATE '2029-12-31' as tract_end_date,
           DATE '2014-01-01' as district_start_date,
           DATE '2025-12-31' as district_end_date
    from nvi.detroit_council_districts cds
    join michigan_tracts tracts
        on st_within(st_centroid(st_transform(tracts.geometry, 2898)), cds.geometry)
    where cds.start_date = DATE '2014-01-01'
    order by cds.district_number;
    """
    )

    df = pd.read_sql(cw_q, db_engine)

    df.to_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2014_cw.csv",
        index=False,
    )
