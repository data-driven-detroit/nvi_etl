from pathlib import Path
import geopandas as gpd
import pandas as pd
from validate import (
    DetroitCouncilDistricts,
    NVINeighborhoodZones,
    TractsToNVICrosswalk,
)
from nvi_etl import db_engine

WORKING_DIR = Path(__file__).resolve().parent


def load_council_districts(logger):
    logger.info("Loading council districts to database.")
    file = gpd.read_file(
        WORKING_DIR / "output" / "council_districts_2026.geojson",
    )

    validated = DetroitCouncilDistricts.validate(file)

    validated.to_postgis(
        "detroit_council_districts", db_engine, schema="nvi", if_exists="append"
    )


def load_neighborhood_zones(logger):
    logger.info("Loading neighborhood zones to database.")
    file = gpd.read_file(
        WORKING_DIR / "output" / "neighborhood_zones_2026.geojson",
    )

    validated = NVINeighborhoodZones.validate(file)

    validated.to_postgis(
        "neighborhood_zones", db_engine, schema="nvi", if_exists="append"
    )


def load_2020_tracts_to_2026_nvi_cw(logger):
    logger.info("Loading transformed file to the nvi schema on the database.")

    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_2020_to_zones_2026_crosswalk.csv",
    )

    validated = TractsToNVICrosswalk.validate(file)

    validated.to_sql(
        "tracts_to_nvi_crosswalk", db_engine, schema="nvi", if_exists="append", index=False
    )

