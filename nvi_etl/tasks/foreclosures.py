"""Foreclosures ETL task.

Current year: runs foreclosures_current.sql to get non-foreclosure percentages
by geography, loads to the value table.

History: runs foreclosures_history.sql to get historical foreclosure counts
by geography and year, loads to the context_values table.
"""

import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import CONF_DIR, SQL_DIR
from nvi_etl.db import get_engine, read_sql_file
from nvi_etl.registry import task, TaskResult
from nvi_etl.reshape import elongate
from nvi_etl.geo import pin_location
from nvi_etl.upsert import upsert_values, upsert_context_values

DATA_YEAR = 2025


@task("foreclosures", phase=1, description="Foreclosure rates and historical counts")
def run(source: Engine, target: Engine) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    ipds_engine = get_engine("ipds")
    primary_indicators = pd.read_csv(CONF_DIR / "ipds" / "primary_indicator_ids.csv")
    context_indicators = pd.read_csv(CONF_DIR / "ipds" / "context_indicator_ids.csv")
    total_rows = 0

    # Current year foreclosures (from SQL, spatial joins in DB)
    logger.info("Running foreclosures_current.sql")
    current_wide = read_sql_file(SQL_DIR / "foreclosures_current.sql", ipds_engine)
    current_wide["year"] = DATA_YEAR
    current_wide["location_id"] = current_wide.apply(pin_location, axis=1)

    current_tall = (
        elongate(current_wide)
        .merge(primary_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .assign(value_type_id=1, survey_id=1)
    )

    total_rows += upsert_values(target, current_tall)

    # Historical foreclosure counts
    logger.info("Running foreclosures_history.sql")
    history_wide = read_sql_file(SQL_DIR / "foreclosures_history.sql", ipds_engine)
    history_wide["location_id"] = history_wide.apply(pin_location, axis=1)

    history_tall = (
        elongate(history_wide)
        .merge(context_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type", "year", "filter_type_id"], axis=1, errors="ignore")
    )

    total_rows += upsert_context_values(target, history_tall)

    return TaskResult(task_name="foreclosures", rows_inserted=total_rows, success=True)
