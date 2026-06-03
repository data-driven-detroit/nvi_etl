from pathlib import Path
import pandas as pd
from nvi_etl import make_engine_for
from nvi_etl.schema import NVIValueTable, NVIContextValueTable


WORKING_DIR = Path(__file__).resolve().parent

SURVEY_VALUES_TABLE = "value"
CONTEXT_VALUES_TABLE = "context_value"

YEAR = 2025

PRIMARY_UNIQUE_TOGETHER = [
    "survey_id",
    "year",
    "value_type_id",
    "indicator_id",
    "survey_question_id",
    "survey_question_option_id",
    "location_id",
]

CONTEXT_UNIQUE_TOGETHER = [
    "source_id",
    "start_date",
    "end_date",
    "indicator_id",
    "filter_option_id",
    "location_id",
]


def filter_existing(frame, table, unique_together, db):
    existing = pd.read_sql(f'SELECT {", ".join(unique_together)} FROM {table}', db)
    merged = frame.merge(existing, on=unique_together, how="left", indicator=True)
    return merged[merged["_merge"] == "left_only"].drop(columns="_merge")


def load_acs(logger):
    db_engine = make_engine_for("nvi_test")

    logger.info(f"Loading survey indicators into {SURVEY_VALUES_TABLE}")
    survey = pd.read_csv(WORKING_DIR / "output" / "acs_primary_indicators_tall.csv")
    survey = NVIValueTable.validate(survey)
    survey["year"] = YEAR
    survey = filter_existing(survey, SURVEY_VALUES_TABLE, PRIMARY_UNIQUE_TOGETHER, db_engine)
    survey.to_sql(SURVEY_VALUES_TABLE, db_engine, index=False, if_exists="append")

    logger.info(f"Loading context indicators into {CONTEXT_VALUES_TABLE}")
    context = pd.read_csv(WORKING_DIR / "output" / "acs_context_indicators_tall.csv")
    context = NVIContextValueTable.validate(context)
    context = filter_existing(context, CONTEXT_VALUES_TABLE, CONTEXT_UNIQUE_TOGETHER, db_engine)
    context.to_sql(CONTEXT_VALUES_TABLE, db_engine, index=False, if_exists="append")