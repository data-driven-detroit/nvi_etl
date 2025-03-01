from pathlib import Path
from datetime import date
import pandas as pd
import pandera as pa
from pandera.typing.geopandas import GeoSeries
from geopandas import gpd


WORKING_DIR = Path(__file__).resolve().parent


class DetroitCouncilDistricts(pa.DataFrameModel):
    district_number: str = pa.Field()
    square_miles: float = pa.Field()
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)
    geometry: GeoSeries = pa.Field()


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


class TractsToCouncilDistricts(pa.DataFrameModel):
    tract_geoid: str = pa.Field()
    district_number: int = pa.Field()
    tract_start_date: date = pa.Field(coerce=True)
    tract_end_date: date = pa.Field(coerce=True)
    district_start_date: date = pa.Field(coerce=True)
    district_end_date: date = pa.Field(coerce=True)


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
