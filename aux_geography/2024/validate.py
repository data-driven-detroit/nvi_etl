from pathlib import Path
from datetime import date
import pandas as pd
import pandera as pa
from pandera.typing.geopandas import GeoSeries
from geopandas import gpd


WORKING_DIR = Path(__file__).resolve().parent


class DetroitCouncilDistricts(pa.DataFrameModel):
    district_number: str = pa.Field(coerce=True)
    square_miles: float = pa.Field()
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)
    geometry: GeoSeries = pa.Field()

    @pa.check("square_miles")
    def check_total_sq_mi(cls, square_miles) -> bool:
        """
        Make this more precise if you remember exactly how bit Detroit is.
        """
        return 120 < square_miles.sum() < 140


def validate_council_districts(logger):
    logger.info("Validating council districts.")

    file = gpd.read_file(
        WORKING_DIR / "output" / "council_districts_2026.geojson"
    )

    validated = DetroitCouncilDistricts.validate(file)

    validated.to_file(
        WORKING_DIR / "output" / "council_districts_2026_validated.geojson"
    )


class NVINeighborhoodZones(pa.DataFrameModel):
    zone_id: str = pa.Field()
    district_number: str = pa.Field(coerce=True)
    square_miles: float = pa.Field()
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)
    geometry: GeoSeries = pa.Field()

    @pa.check("square_miles")
    def check_total_sq_mi(cls, square_miles) -> bool:
        return 120 < square_miles.sum() < 140


def validate_neighborhood_zones(logger):
    logger.info("Validating council districts.")

    file = gpd.read_file(
        WORKING_DIR / "output" / "neighborhood_zones_2017.geojson"
    )

    validated = NVINeighborhoodZones.validate(file)

    validated.to_file(
        WORKING_DIR / "output" / "neighborhood_zones_2017_validated.geojson"
    )


class TractsToCouncilDistricts(pa.DataFrameModel):
    tract_geoid: str = pa.Field()
    district_number: int = pa.Field()
    tract_start_date: date = pa.Field(coerce=True)
    tract_end_date: date = pa.Field(coerce=True)
    district_start_date: date = pa.Field(coerce=True)
    district_end_date: date = pa.Field(coerce=True)


def validate_2010_tract_2026_cd_crosswalk(logger):
    logger.info("Validating 2010 tracts to 2026 council districts.")

    df = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2026_cw.csv"
    )

    validated = TractsToCouncilDistricts.validate(df)

    logger.info("SUCCESS")

    validated.to_csv(
        WORKING_DIR / "output" / "tracts_districts_2010_2026_cw_validated.csv",
        index=False
    )


def validate_2020_tract_2026_cd_crosswalk(logger):
    logger.info("Validating 2020 tracts to 2026 council districts.")

    df = pd.read_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2026_cw.csv"
    )

    logger.info("SUCCESS")

    validated = TractsToCouncilDistricts.validate(df)

    validated.to_csv(
        WORKING_DIR / "output" / "tracts_districts_2020_2026_cw_validated.csv",
        index=False
    )
