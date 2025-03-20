from datetime import date
import pandera as pa
from pandera.typing.geopandas import GeoSeries


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


class TractsToNVICrosswalk(pa.DataFrameModel):
    tract_geoid: str = pa.Field()
    zone_name: str = pa.Field()
    district_number: int = pa.Field()
    tract_start_date: date = pa.Field(coerce=True)
    tract_end_date: date = pa.Field(coerce=True)
    zone_start_date: date = pa.Field(coerce=True)
    zone_end_date: date = pa.Field(coerce=True)
