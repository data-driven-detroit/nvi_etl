from pathlib import Path
import geopandas as gpd
import pandas as pd
from validate import NVINeighborhoodZones
from nvi_etl import db_engine

WORKING_DIR = Path(__file__).resolve().parent


def load_neighborhood_zones(logger):
    logger.info("Loading neighborhood zones to database.")
    file = gpd.read_file(
        WORKING_DIR / "output" / "neighborhood_zones_2026.geojson",
    )

    validated = NVINeighborhoodZones.validate(file)

    validated.to_postgis(
        "neighborhood_zones", db_engine, schema="nvi", if_exists="append"
    )

