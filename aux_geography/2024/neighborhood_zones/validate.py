from datetime import date
import pandera as pa
from pandera.typing.geopandas import GeoSeries


class NVINeighborhoodZones(pa.DataFrameModel):
    zone_id: str = pa.Field()
    district_number: int = pa.Field()
    square_miles: float = pa.Field()
    start_date: date = pa.Field()
    end_date: date = pa.Field()
    geometry: GeoSeries = pa.Field()
