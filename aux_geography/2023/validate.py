from pathlib import Path
from datetime import date
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
    pass

