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
            "field_reference_council_districts.json"
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
        gpd.read_file(config["source_files"]["neighborhood_zones"])
        .to_crs(crs="EPSG:2898")
        .rename(columns=field_reference["rename"])
        .assign(
            start_date=date.fromisoformat("2026-01-01"),
            end_date=date.fromisoformat("2036-12-31"),
            square_miles=calculate_square_miles
        )
    )[field_reference["order"]]
    
    nzs.to_file(WORKING_DIR / "output" / "neighborhood_zones_2026.geojson")


def transform_2020_tracts_to_2026_nvi_cw(logger):
    logger.info("Transforming tracts to council_districts / neighborhood zones crosswalk.")
    
    field_reference = json.loads(
        (
            WORKING_DIR / 
            "conf" / 
            "field_reference_tract_zone_crosswalk.json"
        ).read_text()
    )

    nzs = (
        gpd.read_file(config["source_files"]["district_zone_tract_crosswalk"])
        .set_crs(crs="EPSG:2898")
        .rename(columns=field_reference["renames"])
        .assign(
            tract_geoid = lambda df: "14000US" + df["__tract_geoid"],
            tract_start_date=date.fromisoformat("2020-01-01"),
            tract_end_date=date.fromisoformat("2029-12-31"),
            zone_start_date=date.fromisoformat("2026-01-01"),
            zone_end_date=date.fromisoformat("2036-12-31"),
        )
    )[field_reference["column_order"]]

    nzs.to_csv(
        WORKING_DIR / "output" / "tracts_2020_to_zones_2026_crosswalk.csv", index=False
    )
