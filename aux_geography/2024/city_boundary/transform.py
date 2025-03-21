import json
import configparser
from datetime import date
from pathlib import Path
import geopandas as gpd

from nvi_etl.utilities import calculate_square_miles


WORKING_DIR = Path(__file__).resolve().parent

config = configparser.ConfigParser()
config.read(WORKING_DIR / "conf" / ".conf")


def transform_city_boundary(logger):
    logger.info("Transforming neighborhood zones.")
    
    field_reference = json.loads(
        (WORKING_DIR / "conf" / "field_reference.json").read_text()
    )


    boundary = (
        gpd.read_file(config["source_files"]["city_boundary"])
        .to_crs(crs="EPSG:2898")
        .rename(columns=field_reference["renames"]) # This is empty
        .assign(
            geoid="06000US2616322000",
            start_date=date.fromisoformat("2026-01-01"),
            end_date=date.fromisoformat("2099-12-31"), # This probably wont change this century
            square_miles=calculate_square_miles
        )
    )[field_reference["column_order"]]
    
    boundary.to_file(WORKING_DIR / "output" / "city_boundary_2026.geojson")

