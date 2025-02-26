import pandera as pa


class NVIValueTable(pa.DataFrameModel):
    """
    This is the schema that we're transforming all of our data to. It's
    the center table of the NVI DB diagram (add link?).
    """
    survey_id: int = pa.Field(nullable=True)
    year: int = pa.Field(nullable=False)
    indicator_id: int = pa.Field(nullable=False)
    survey_question_id: int = pa.Field(nullable=True)
    survey_question_option_id: int = pa.Field(nullable=True)
    location_id: int = pa.Field(nullable=False)
    count: float = pa.Field(nullable=True)
    universe: float = pa.Field(nullable=True)
    percentage: float = pa.Field(nullable=True)
    rate: float = pa.Field(nullable=True)
    rate_per: float = pa.Field(nullable=True)
    dollars: float = pa.Field(nullable=True)

    # TODO: Need to provide the key combo to the Config class, but I 
    # can't remember the syntax
    # [year, location_id, indicator_id] need to be unique
        


