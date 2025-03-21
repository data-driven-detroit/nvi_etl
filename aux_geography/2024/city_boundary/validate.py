from datetime import date
import pandera as pa
from pandera.typing.geopandas import GeoSeries


class DetroitCityBoundary(pa.DataFrameModel):
    geoid: str = pa.Field()
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)
    square_miles: float = pa.Field()
    geometry: GeoSeries = pa.Field()

