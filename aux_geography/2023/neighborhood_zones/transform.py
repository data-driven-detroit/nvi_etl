import json
import configparser
from datetime import date
from pathlib import Path
import geopandas as gpd

from nvi_etl.utilities import calculate_square_miles

WORKING_DIR = Path(__file__).resolve().parent

config = configparser.ConfigParser()
config.read(WORKING_DIR / "conf" / ".conf")


def transform_neighborhood_zones(logger):
    logger.info("Transforming neighborhood zones.")
    
    field_reference = json.loads(
        (WORKING_DIR / "conf" / "field_reference.json").read_text()
    )

    nzs = (
        gpd.read_file(config["source_files"]["neighborhood_zones"])
        .to_crs(crs="EPSG:2898")
        .rename(columns=field_reference["rename"])
        .assign(
            start_date=date.fromisoformat("2022-01-01"),
            end_date=date.fromisoformat("2025-12-31"),
            square_miles=calculate_square_miles
        )
    )[field_reference["order"]]
    
    nzs.to_file(WORKING_DIR / "output" / "neighborhood_zones_2018.geojson")

