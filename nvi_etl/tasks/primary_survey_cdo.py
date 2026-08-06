"""Primary NVI survey ETL task -- CDO boundary aggregation.

Reads the same survey CSV + geocoded shapefile as primary_survey, but
performs spatial joins to CDO service areas instead of council districts
and zones.  Output is written to CSV, not the database.
"""

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import CONF_DIR, DUA_FOLDER
from nvi_etl.registry import task, TaskResult
from nvi_etl.geo import pull_cdo_boundaries
from nvi_etl.tasks.primary_survey import (
    SURVEY_YEAR,
    SURVEY_CONF,
    VALUE_COLUMNS,
    OUTPUT_COLUMN_ORDER,
    combine_survey_and_geocoded,
    create_indicator_rows,
    create_question_rows,
)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "survey" / "output"


def add_cdo_boundaries(geocoded, cdo_boundaries):
    """Spatial join geocoded survey points to CDO service areas.

    CDO boundaries can overlap, so a single respondent may appear in
    multiple CDO rows.  Dedup on (response_id, organization_name) to
    avoid double-counting within a single CDO.
    """
    return (
        geocoded.to_crs(2898)
        .sjoin(
            cdo_boundaries[["geometry", "organization_name"]].to_crs(2898),
            how="inner",
            predicate="within",
        )
        .drop(columns="index_right")
        .drop_duplicates(subset=["response_id", "organization_name"])
    )


@task("primary_survey_cdo", phase=2, description="Primary NVI survey -- CDO boundary aggregation")
def run(source: Engine, target: Engine) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    # Read source files -- same paths as primary_survey
    survey_csv = os.environ.get(
        "NVI_SURVEY_CSV",
        str(DUA_FOLDER / "3_Projects" / "NVI" / "2025" / "nvi_survey_data_2025_20260226.csv"),
    )
    geocoded_shp = os.environ.get(
        "NVI_GEOCODED_SHP",
        str(DUA_FOLDER / "3_Projects" / "NVI" / "2025" / "Final Shapefiles" / "Final2025NVIDataset_cleaned_20260304.shp"),
    )

    logger.info("Opening survey files for CDO aggregation")
    frame = pd.read_csv(survey_csv, low_memory=False).rename(
        columns={"Response ID": "response_id"}
    )
    geoframe = gpd.read_file(geocoded_shp).rename(
        columns={"Response_I": "response_id"}
    )
    datadictionary = pd.read_excel(SURVEY_CONF / "nvi_answer_key_20260316.xlsx")
    location_dictionary = pd.read_excel(
        SURVEY_CONF / "cdo_locations.xlsx",
        dtype={"location": str},
    )

    geocoded = combine_survey_and_geocoded(frame, geoframe)

    # Spatial join to CDO boundaries
    logger.info("Pulling CDO boundaries for aggregation")
    cdo_boundaries = pull_cdo_boundaries(source)
    complete_frame = add_cdo_boundaries(geocoded, cdo_boundaries)

    survey_date = pd.Timestamp(year=SURVEY_YEAR, month=1, day=1)
    summaries = ["organization_name"]

    # Indicator aggregation
    logger.info("Creating CDO indicator rows")
    indicators, errors = create_indicator_rows(
        complete_frame, datadictionary, survey_date, summaries
    )
    if errors:
        for err_id, err_msg in errors:
            logger.warning(f"Indicator {err_id}: {err_msg}")

    indicator_output = (
        indicators.rename(columns={"location_id": "location"})
        .merge(location_dictionary, on="location", how="left")
        .astype({"location_id": pd.Int64Dtype()})
        .assign(
            rate=float("nan"), rate_per=float("nan"), dollars=float("nan"),
            survey_id=1, index=float("nan"),
            survey_question_id=pd.NA, survey_question_option_id=pd.NA,
        )
        .dropna(subset=["location_id", "indicator_id"])
        .astype({
            "survey_question_id": pd.Int64Dtype(),
            "survey_question_option_id": pd.Int64Dtype(),
        })
    )[VALUE_COLUMNS]

    # Question aggregation
    logger.info("Creating CDO question rows")
    table = create_question_rows(
        complete_frame, datadictionary, survey_date, summaries
    )
    question_output = (
        table.merge(location_dictionary, on="location", how="left")
        .astype({"location_id": pd.Int64Dtype(), "survey_code": pd.Int64Dtype()})
    )[OUTPUT_COLUMN_ORDER]

    # Write to CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    indicators_path = OUTPUT_DIR / f"primary_survey_cdo_indicators_{SURVEY_YEAR}.csv"
    questions_path = OUTPUT_DIR / f"primary_survey_cdo_questions_{SURVEY_YEAR}.csv"

    indicator_output.to_csv(indicators_path, index=False)
    question_output.to_csv(questions_path, index=False)

    total_rows = len(indicator_output) + len(question_output)
    logger.info(f"Wrote {len(indicator_output)} indicator rows to {indicators_path}")
    logger.info(f"Wrote {len(question_output)} question rows to {questions_path}")

    return TaskResult(task_name="primary_survey_cdo", rows_inserted=total_rows, success=True)
