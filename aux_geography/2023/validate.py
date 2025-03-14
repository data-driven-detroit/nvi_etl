from pathlib import Path
import pandas as pd
from geopandas import gpd


WORKING_DIR = Path(__file__).resolve().parent


def validate_council_districts(logger):
    logger.info("Validating council districts.")

    file = gpd.read_file(
        WORKING_DIR / "output" / "council_districts_2014.geojson"
    )

    validated = DetroitCouncilDistricts.validate(file)

    validated.to_file(
        WORKING_DIR / "output" / "council_districts_2014_validated.geojson"
    )


def validate_neighborhood_zones(logger):
    """
    We haven't seen these yet -- hopefully we'll get them next week.
    """


def validate_2010_tract_2014_cd_crosswalk(logger):
    logger.info("Validating 2010 tracts to 2014 council districts.")

    df = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2014_cw.csv"
    )

    validated = TractsToCouncilDistricts.validate(df)

    logger.info("SUCCESS")

    validated.to_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2014_cw_validated.csv",
        index=False
    )

def validate_2020_tract_2014_cd_crosswalk(logger):
    logger.info("Validating 2020 tracts to 2014 council districts.")

    df = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2014_cw.csv"
    )

    logger.info("SUCCESS")

    validated = TractsToCouncilDistricts.validate(df)

    validated.to_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2014_cw_validated.csv",
        index=False
    )
