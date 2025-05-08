import json
import configparser
from datetime import date
from pathlib import Path
import geopandas as gpd

from nvi_etl.utilities import calculate_square_miles

WORKING_DIR = Path(__file__).resolve().parent

config = configparser.ConfigParser()
config.read(WORKING_DIR / "conf" / ".conf")


def transform_council_districts(logger):
    logger.info("Transforming council districts.")

    field_reference = json.loads(
        (
            WORKING_DIR / 
            "conf" / 
            "field_reference.json"
        ).read_text()
    )

    cds = (
        gpd.read_file(config["source_files"]["council_districts"])
        .rename(columns=field_reference["renames"])
        .to_crs(crs="EPSG:2898")
        .assign(
            start_date=date.fromisoformat("2014-01-01"),
            end_date=date.fromisoformat("2025-12-31"), # 'Forever' wasn't working
            square_miles=calculate_square_miles
        )
    )[field_reference["column_order"]]

    cds.to_file(WORKING_DIR / "output" / "council_districts_2014.geojson")
