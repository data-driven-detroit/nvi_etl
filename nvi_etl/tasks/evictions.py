"""Eviction data ETL task."""

import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import CONF_DIR, SQL_DIR
from nvi_etl.db import get_engine, read_sql_file
from nvi_etl.registry import task, TaskResult
from nvi_etl.geo import pin_location
from nvi_etl.schema import CONTEXT_VALUE_COLUMNS, CONTEXT_VALUES_TABLE


@task("evictions", phase=1, description="Eviction counts by geography and year")
def run(source: Engine, target: Engine) -> TaskResult:
    evictions_db = get_engine("evictions")
    ipds_db = get_engine("ipds")

    # Extract
    evictions = read_sql_file(SQL_DIR / "evictions.sql", evictions_db)
    parcels = read_sql_file(SQL_DIR / "parcel_labels.sql", ipds_db)
    point_level = evictions.merge(parcels, on="d3_id", how="inner")

    # Transform
    point_level["year"] = pd.to_datetime(point_level["filed_date"]).dt.year
    context_indicators = pd.read_csv(CONF_DIR / "evictions" / "context_indicator_ids.csv")

    aggregated = pd.concat([
        (
            point_level.groupby(["zone_id", "year"])
            .size().rename("count").reset_index()
            .rename(columns={"zone_id": "geography"})
            .assign(geo_type="zone")
        ),
        (
            point_level.groupby(["district_number", "year"])
            .size().rename("count").reset_index()
            .rename(columns={"district_number": "geography"})
            .astype({"geography": "str"})
            .assign(geo_type="district")
        ),
        (
            point_level.groupby(["year"])
            .size().rename("count").reset_index()
            .assign(geo_type="citywide", geography="Detroit")
        ),
    ])

    result = (
        aggregated
        .astype({"year": pd.Int64Dtype()})
        .assign(location_id=lambda df: df.apply(pin_location, axis=1))
        .merge(context_indicators, on=["year"])
        .assign(
            universe=pd.NA, percentage=pd.NA, rate=pd.NA,
            rate_per=pd.NA, dollars=pd.NA, index=pd.NA,
        )
        [CONTEXT_VALUE_COLUMNS]
    )

    # Load
    result.to_sql(CONTEXT_VALUES_TABLE, target, if_exists="append", index=False)

    return TaskResult(task_name="evictions", rows_inserted=len(result), success=True)
