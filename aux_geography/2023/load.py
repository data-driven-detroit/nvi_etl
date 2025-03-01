from pathlib import Path
import geopandas as gpd
import pandas as pd
from validate import DetroitCouncilDistricts, TractsToCouncilDistricts
from nvi_etl import db_engine

WORKING_DIR = Path(__file__).resolve().parent


def load_council_districts(logger):
    logger.info("Loading council districts to database.")
    file = gpd.read_file(
        WORKING_DIR / "output" / "council_districts_2014_validated.geojson",
    )

    correct_types = DetroitCouncilDistricts.validate(file)

    correct_types.to_postgis("detroit_council_districts", db_engine, schema="nvi", if_exists="append")


def load_neighborhood_zones(logger):
    pass


def load_2010_tracts_to_2014_council_districts(logger):
    logger.info("Loading 2010-2026 crosswalk into database")
    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2014_cw_validated.csv"
    )

    validated = TractsToCouncilDistricts.validate(file)

    validated.to_sql(
        "tracts_to_council_districts",
        db_engine,
        schema="nvi",
        if_exists="append",
        index=False,
    )


def load_2020_tracts_to_2014_council_districts(logger):
    logger.info("Loading 2020-2026 crosswalk into database")
    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2014_cw_validated.csv"
    )

    validated = TractsToCouncilDistricts.validate(file)

    validated.to_sql(
        "tracts_to_council_districts",
        db_engine,
        schema="nvi",
        if_exists="append",
        index=False,
    )
