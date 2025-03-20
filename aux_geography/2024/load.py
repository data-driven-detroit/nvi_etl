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


def load_2020_tracts_to_2026_nvi_cw(logger):

    validated = 
    pass