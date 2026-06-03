from pathlib import Path
import pandas as pd
from nvi_etl import make_engine_for
from logging import Logger

WORKING_DIR = Path(__file__).resolve().parent


def load(logger: Logger):
    logger.info("Loading data into nvi_test.")
    db = make_engine_for("nvi_test")

    key_columns = (
        "source_id",
        "start_date",
        "end_date",
        "indicator_id",
        "filter_option_id",
        "location_id",
    )

    frame = pd.read_csv(WORKING_DIR / "output" / "acs_context_indicators_tall.csv")
    frame[["start_date", "end_date"]] = frame[["start_date", "end_date"]].apply(pd.to_datetime)

    existing = pd.read_sql(f'SELECT {", ".join(key_columns)} FROM context_value', db)
    existing[["start_date", "end_date"]] = existing[["start_date", "end_date"]].apply(pd.to_datetime)

    new = frame.merge(existing, on=key_columns, how="left", indicator=True)
    new = new[new["_merge"] == "left_only"].drop(columns="_merge")

    new.to_sql("context_value", db, if_exists="append", index=False)