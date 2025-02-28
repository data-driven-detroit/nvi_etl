import json
from datetime import date
from pathlib import Path
import geopandas as gpd


WORKING_DIR = Path(__file__).resolve().parent

field_reference = json.loads(
    (WORKING_DIR / "conf" / "field_reference.json").read_text()
)


def transform_council_districts(logger):
    logger.info("Transforming council districts.")

    cds = (
        gpd.read_file(
            WORKING_DIR / "input" / "DetroitCouncilDistricts2025.zip"
        )
        .rename(columns=field_reference["rename"])
        .assign(
            start_date=date.fromisoformat("2014-01-01"),
            end_date=date.fromisoformat("2025-12-31"),
        )
    )[field_reference["order"]]

    cds.to_file(WORKING_DIR / "output" / "council_districts_2014.geojson")


def transform_neighborhood_zones(logger):
    logger.info("Transforming neighborhood zones.")

    cds = (
        gpd.read_file(
            WORKING_DIR / "input" / "NVI Boundaries.zip"
        )
        .rename(columns=field_reference["rename"])
        .assign(
            start_date=date.fromisoformat("2014-01-01"),
            end_date=date.fromisoformat("2025-12-31"),
        )
    )[field_reference["order"]]

    cds.to_file(WORKING_DIR / "output" / "neighborhood_zones_2016.geojson")


def transform_2020_tracts_to_2014_council_districts(logger):
    logger.info("No transform required for tracts to districts.")


def transform_2010_tracts_to_2014_council_districts(logger):
    logger.info("No transform required for tracts to districts.")
