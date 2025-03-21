import json
import configparser
from datetime import date
from pathlib import Path
import geopandas as gpd


WORKING_DIR = Path(__file__).resolve().parent

config = configparser.ConfigParser()
config.read(WORKING_DIR / "conf" / ".conf")


def calculate_square_miles(gdf):
    """
    This relies on the fact the the gdf is in the NVI-standard 
    projection EPSG:2898. The unit for this projection is feet,
    so the calculation for square miles is:

    1 mile = 5,280 feet
    1 square mile = 27,878,400 (5,280²) square feet

    area sqmi = area sqft / 27,878,400 sqft/sqmi 
                            (2.788e7 as a float)

    This will throw a warning for geographic projections like web
    mercator, and also give pretty bad results.
    """
    
    return gdf.geometry.area / 2.788e7


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
            start_date=date.fromisoformat("2026-01-01"),
            end_date=date.fromisoformat("2036-12-31"), # 'Forever' wasn't working
            square_miles=calculate_square_miles
        )
    )[field_reference["column_order"]]

    cds.to_file(WORKING_DIR / "output" / "council_districts_2026.geojson")
