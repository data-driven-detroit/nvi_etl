import pandera as pa
import pandas as pd
from datetime import date
import pandera as pa
from pandera.typing.geopandas import GeoSeries


class NVIValueTable(pa.DataFrameModel):
    """
    This is the schema that we're transforming all of our data to. It's
    the center table of the NVI DB diagram (add link?).
    """
    survey_id: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    year: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    indicator_id: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    survey_question_id: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    survey_question_option_id: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    location_id: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    count: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    universe: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    percentage: float = pa.Field(nullable=True)
    rate: float = pa.Field(nullable=True)
    rate_per: float = pa.Field(nullable=True)
    dollars: float = pa.Field(nullable=True)
    index: float = pa.Field(nullable=True)
    
    class Config:
        strict=True
        unique=[
            "indicator_id",
            "location_id",
            "survey_id",
            "survey_question_id",
            "survey_question_option_id",
            "year",
        ]

    # TODO: Need to provide the key combo to the Config class, but I 
    # can't remember the syntax
    # [year, location_id, indicator_id] need to be unique


class NVIContextValueTable(pa.DataFrameModel):
    """
    The NVI Context values are the indicators from sources other than the
    NVI survey. They have slightly different structure than the primary 
    indicator
    """
    location_id: pd.Int64Dtype = pa.Field(coerce=True)
    indicator_id: pd.Int64Dtype = pa.Field(coerce=True)
    filter_type_id: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    filter_option_id: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    source_id: pd.Int64Dtype = pa.Field(coerce=True)
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)
    count: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    universe: pd.Int64Dtype = pa.Field(nullable=True, coerce=True)
    percentage: float = pa.Field(nullable=True)
    rate: float = pa.Field(nullable=True)
    rate_per: float = pa.Field(nullable=True)
    dollars: float = pa.Field(nullable=True)
    index: float = pa.Field(nullable=True)

    class Config:
        strict=True
        unique=[
            "location_id",
            "indicator_id",
            "filter_type_id",
            "filter_option_id",
            "source_id",
            "start_date",
            "end_date",
        ]

# These are for the aux_geography module, for enabling the rest of the ETL

class DetroitCouncilDistricts(pa.DataFrameModel):
    district_number: str = pa.Field()
    square_miles: float = pa.Field()
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)
    geometry: GeoSeries = pa.Field()


class TractsToCouncilDistricts(pa.DataFrameModel):
    tract_geoid: str = pa.Field()
    district_number: int = pa.Field()
    tract_start_date: date = pa.Field(coerce=True)
    tract_end_date: date = pa.Field(coerce=True)
    district_start_date: date = pa.Field(coerce=True)
    district_end_date: date = pa.Field(coerce=True)


class NVINeighborhoodZones(pa.DataFrameModel):
    zone_id: str = pa.Field()
    district_number: int = pa.Field()
    square_miles: float = pa.Field()
    start_date: date = pa.Field()
    end_date: date = pa.Field()
    geometry : GeoSeries = pa.Field()


class TractsToNVIZones(pa.DataFrameModel):
    tract_geoid: str = pa.Field()
    zone_id: str = pa.Field()
    district_number: int = pa.Field()
    tract_start_date: date = pa.Field()
    tract_end_date: date = pa.Field()
    district_start_date: date = pa.Field()
    district_end_date : date = pa.Field()

