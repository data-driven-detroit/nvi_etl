import json
from datetime import date
from pathlib import Path
import geopandas as gpd


WORKING_DIR = Path(__file__).resolve().parent


def calculate_square_miles(gdf):
    """
    This fully copies the dataframe, so don't use this for huge datasets.
    """

    projected = gdf.to_crs(crs="EPSG:2253") # The unit is feet
    
    return projected.geometry.area / 2.788e7


def transform_council_districts(logger):
    logger.info("Transforming council districts.")

    field_reference = json.loads(
        (
            WORKING_DIR / 
            "conf" / 
            "field_reference_council_districts.json"
        ).read_text()
    )

    cds = (
        gpd.read_file(
            WORKING_DIR / "input" / "Detroit_City_Council_Districts_2026.geojson"
        )
        .to_crs(crs="EPSG:2898") # TODO: This needs to be organized! 2898 for everyhting?
        .assign(
            start_date=date.fromisoformat("2026-01-01"),
            end_date=date.fromisoformat("2036-12-31"), # 'Forever' wasn't working
            square_miles=calculate_square_miles
        )
    )[field_reference["order"]]
    
    cds.to_file(WORKING_DIR / "output" / "council_districts_2026.geojson")


def transform_neighborhood_zones(logger):
    logger.info("Transforming neighborhood zones.")
    
    field_reference = json.loads(
        (
            WORKING_DIR / 
            "conf" / 
            "field_reference_neighborhood_zones.json"
        ).read_text()
    )

    nzs = (
        gpd.read_file(
            WORKING_DIR / "input" / "nvi_zones_20250301.zip"
        )
        .rename(columns=field_reference["rename"])
        .assign(
            start_date=date.fromisoformat("2017-01-01"),
            end_date=date.fromisoformat("2025-12-31"),
            square_miles=calculate_square_miles
        )
    )[field_reference["order"]]

    nzs.to_file(WORKING_DIR / "output" / "neighborhood_zones_2026.geojson")


def transform_2020_tracts_to_2026_council_districts(logger):
    logger.info("No transformation necessary for tracts to cds")


def transform_2010_tracts_to_2026_council_districts(logger):
    logger.info("No transformation necessary for tracts to cds")
