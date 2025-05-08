from datetime import date
import pandas as pd
import pandera as pa
from pandera.typing.geopandas import GeoSeries


class NVINeighborhoodZones(pa.DataFrameModel):
    zone_id: str = pa.Field()
    district_number: pd.Int32Dtype = pa.Field()
    square_miles: float = pa.Field()
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)
    geometry: GeoSeries = pa.Field()
