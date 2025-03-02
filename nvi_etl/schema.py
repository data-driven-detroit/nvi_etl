import pandera as pa
import pandas as pd


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
    count: float = pa.Field(nullable=True)
    universe: float = pa.Field(nullable=True)
    percentage: float = pa.Field(nullable=True)
    rate: float = pa.Field(nullable=True)
    rate_per: float = pa.Field(nullable=True)
    dollars: float = pa.Field(nullable=True)
    index: float = pa.Field(nullable=True)
    
    class Config:
        strict=True

    # TODO: Need to provide the key combo to the Config class, but I 
    # can't remember the syntax
    # [year, location_id, indicator_id] need to be unique

