"""Michigan school data -- 3rd grade ELA proficiency."""

import json
from datetime import date

import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import CONF_DIR, SQL_DIR
from nvi_etl.db import read_sql_file
from nvi_etl.registry import task, TaskResult
from nvi_etl.reshape import elongate
from nvi_etl.schema import NVIValueTable, SURVEY_VALUES_TABLE
from nvi_etl.upsert import upsert_values

START_DATE = date(2024, 7, 1)
GEOMETRY_START_DATE = date(2026, 1, 1)
YEAR = 2025


@task("mischooldata", phase=1, description="3rd grade ELA proficiency from MI school data")
def run(source: Engine, target: Engine) -> TaskResult:
    location_map = json.loads(
        (CONF_DIR / "mischooldata" / "location_map.json").read_text()
    )
    primary_indicators = pd.read_csv(
        CONF_DIR / "mischooldata" / "primary_indicator_ids.csv"
    )

    def pin_location_id(row):
        return location_map[row["geo_type"]][row["geography"]]

    # Extract
    params = {
        "start_date": START_DATE,
        "geometry_start_date": GEOMETRY_START_DATE,
        "year": YEAR,
    }
    wide_file = read_sql_file(
        SQL_DIR / "third_grade_ela_combined.sql", source, params=params
    )

    # Transform
    wide_file["location_id"] = wide_file.apply(pin_location_id, axis=1)

    tall = (
        elongate(wide_file)
        .merge(primary_indicators, on=["indicator", "year"], how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .assign(value_type_id=1, survey_id=1)
    )

    # Load
    validated = NVIValueTable.validate(tall)
    validated.to_sql(
        SURVEY_VALUES_TABLE, target, schema="public", if_exists="append", index=False
    )

    return TaskResult(task_name="mischooldata", rows_inserted=len(validated), success=True)
