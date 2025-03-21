from pathlib import Path
import geopandas as gpd
import pandas as pd
from validate import DetroitCityBoundary
from nvi_etl import db_engine

WORKING_DIR = Path(__file__).resolve().parent


def load_city_boundary(logger):
    logger.info("Loading detroit city boundary to database.")
    file = gpd.read_file(
        WORKING_DIR / "output" / "city_boundary_2026.geojson",
    )

    validated = DetroitCityBoundary.validate(file)

    validated.to_postgis(
        "city_boundary", db_engine, schema="nvi", if_exists="append"
    )

