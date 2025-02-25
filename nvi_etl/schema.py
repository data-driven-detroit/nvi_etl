import pandera as pa


class NVIValueTable(pa.DataFrameModel):
    survey_id: int = pa.Field(nullable=True)
    question_id: int = pa.Field(nullable=True)
    year: int = pa.Field()
    location_id: int = pa.Field()
    value: float # ?
