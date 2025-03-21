from pathlib import Path
import geopandas as gpd
import pandas as pd
from validate import DetroitCouncilDistricts
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