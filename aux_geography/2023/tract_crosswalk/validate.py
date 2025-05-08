from datetime import date
import pandera as pa


class TractsToNVICrosswalk(pa.DataFrameModel):
    tract_geoid: str = pa.Field()
    zone_name: str = pa.Field()
    district_number: int = pa.Field()
    tract_start_date: date = pa.Field(coerce=True)
    tract_end_date: date = pa.Field(coerce=True)
    zone_start_date: date = pa.Field(coerce=True)
    zone_end_date: date = pa.Field(coerce=True)