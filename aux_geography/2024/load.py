from pathlib import Path
import geopandas as gpd
import pandas as pd
from validate import (
    DetroitCouncilDistricts,
    NVINeighborhoodZones,
    TractsToCouncilDistricts,
    TractsToNeighborhoodZones,
)
from nvi_etl import db_engine

WORKING_DIR = Path(__file__).resolve().parent


def load_council_districts(logger):
    logger.info("Loading council districts to database.")
    file = gpd.read_file(
        WORKING_DIR / "output" / "council_districts_2026_validated.geojson",
    )

    correct_types = DetroitCouncilDistricts.validate(file)

    correct_types.to_postgis(
        "detroit_council_districts", db_engine, schema="nvi", if_exists="append"
    )


def load_neighborhood_zones(logger):
    logger.info("Loading council districts to database.")
    file = gpd.read_file(
        WORKING_DIR / "output" / "neighborhood_zones_2026_validated.geojson",
    )

    correct_types = NVINeighborhoodZones.validate(file)

    correct_types.to_postgis(
        "neighborhood_zones", db_engine, schema="nvi", if_exists="append"
    )


def load_2010_tracts_to_2026_council_districts(logger):
    logger.info("Loading 2010-2026 crosswalk into database")
    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2026_cw_validated.csv"
    )

    validated = TractsToCouncilDistricts.validate(file)

    validated.to_sql(
        "tracts_to_council_districts",
        db_engine,
        schema="nvi",
        if_exists="append",
        index=False,
    )


def load_2020_tracts_to_2026_council_districts(logger):
    logger.info("Loading 2020-2026 crosswalk into database")
    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2026_cw.csv"
    )

    validated = TractsToCouncilDistricts.validate(file)

    validated.to_sql(
        "tracts_to_council_districts",
        db_engine,
        schema="nvi",
        if_exists="append",
        index=False,
    )


def load_2010_tracts_to_2026_council_districts(logger):
    logger.info("Loading 2010-2026 crosswalk into database")
    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2026_cw_validated.csv"
    )

    validated = TractsToCouncilDistricts.validate(file)

    validated.to_sql(
        "tracts_to_council_districts",
        db_engine,
        schema="nvi",
        if_exists="append",
        index=False,
    )


def load_2020_tracts_to_2026_council_districts(logger):
    logger.info("Loading 2020-2026 tract to district crosswalk into database")
    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2026_cw.csv"
    )

    validated = TractsToCouncilDistricts.validate(file)

    validated.to_sql(
        "tracts_to_council_districts",
        db_engine,
        schema="nvi",
        if_exists="append",
        index=False,
    )


def load_2010_tracts_to_2026_nvi_zones(logger):
    logger.info("NOT loading 2010-2026 tract to zone crosswalk into database.")
    logger.info("This is ready to load -- but skipping to avoid db confusion until everything is communicated.")


def load_2020_tracts_to_2026_nvi_zones(logger):
    logger.info("Loading 2020-2026 tract to zone crosswalk into database")


    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_zones_2020_2026_cw.csv"
    )

    validated = TractsToNeighborhoodZones.validate(file)

    validated.to_sql(
        "tracts_to_nvi_zones",
        db_engine,
        schema="nvi",
        if_exists="append",
        index=False,
    )
