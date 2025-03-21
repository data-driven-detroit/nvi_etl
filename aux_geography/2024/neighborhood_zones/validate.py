from datetime import date
import pandera as pa
from pandera.typing.geopandas import GeoSeries


class DetroitCityBoundary(pa.DataFrameModel):
    start_date: date = pa.Field()
    end_date: date = pa.Field()
    geometry: GeoSeries = pa.Field()