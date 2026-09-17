"""Compile a single human-readable survey document across all geographies.

Aggregates survey data to citywide, district, and zone levels and outputs
a single CSV with topic text, question text, answer text, counts, and
small-cell suppression.
"""

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import DUA_FOLDER
from nvi_etl.registry import task, TaskResult
from nvi_etl.geo import pull_council_districts, pull_zones
from nvi_etl.tasks.primary_survey import (
    ANSWER_KEY,
    SURVEY_YEAR,
    SURVEY_CONF,
    combine_survey_and_geocoded,
    add_districts_and_zones,
    create_indicator_rows,
    create_question_rows,
)
from nvi_etl.tasks.primary_survey_cdo import (
    _build_indicator_block,
    _build_question_block,
    pull_indicator_names,
    suppress_small_cells,
)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "survey" / "output"

SHARED_COLUMNS = [
    "location", "summary_level", "indicator_name", "topic_text", "question_text",
    "answer", "count", "universe", "percentage",
]


@task("survey_compiled", phase=2, description="Single compiled survey document -- all geographies, human-readable")
def run(source: Engine, target: Engine, **kwargs) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    survey_csv = os.environ.get(
        "NVI_SURVEY_CSV",
        str(DUA_FOLDER / "3_Projects" / "NVI" / "2025" / "DUA Data" / "nvi_survey_data_2025_20260226.csv"),
    )
    geocoded_shp = os.environ.get(
        "NVI_GEOCODED_SHP",
        str(DUA_FOLDER / "3_Projects" / "NVI" / "2025" / "DUA Data" / "Final Shapefiles" / "Final2025NVIDataset_cleaned_20260304.shp"),
    )

    logger.info("Opening survey files for compiled document")
    frame = pd.read_csv(survey_csv, low_memory=False).rename(
        columns={"Response ID": "response_id"}
    )
    geoframe = gpd.read_file(geocoded_shp).rename(
        columns={"Response_I": "response_id"}
    )
    datadictionary = pd.read_excel(ANSWER_KEY)

    geocoded = combine_survey_and_geocoded(frame, geoframe)

    nvi_db = kwargs.get("nvi_db")
    indicator_names = None
    if nvi_db is not None:
        logger.info("Pulling indicator names from NVI database")
        indicator_names = pull_indicator_names(nvi_db)

    logger.info("Pulling districts and zones for spatial join")
    districts = pull_council_districts(source, 2026)
    zones = pull_zones(source, 2026)
    complete_frame = add_districts_and_zones(geocoded, districts, zones)

    survey_date = pd.Timestamp(year=SURVEY_YEAR, month=1, day=1)
    summaries = ["citywide", "district_number", "zone_id"]

    indicator_blocks = []
    question_blocks = []
    level_labels = {
        "citywide": "citywide",
        "district_number": "district",
        "zone_id": "zone",
    }

    question_cols = [c for c in SHARED_COLUMNS if c != "indicator_name" or indicator_names is not None]
    indicator_cols = [c for c in question_cols if c not in ("topic_text", "question_text", "answer")]

    for summary in summaries:
        label = level_labels[summary]
        logger.info(f"Aggregating indicators for {label}")

        indicators, errors = _build_indicator_block(
            complete_frame, datadictionary, survey_date, [summary], "location",
            indicator_names=indicator_names,
        )
        indicators = indicators.assign(summary_level=label)
        if errors:
            for err_id, err_msg in errors:
                logger.warning(f"Indicator {err_id} ({label}): {err_msg}")

        logger.info(f"Aggregating questions for {label}")
        questions = _build_question_block(
            complete_frame, datadictionary, survey_date, [summary], "location",
            indicator_names=indicator_names,
        )
        questions = questions.assign(summary_level=label)

        indicator_blocks.append(indicators[indicator_cols])
        question_blocks.append(questions[question_cols])

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total_rows = 0

    indicators_combined = suppress_small_cells(pd.concat(indicator_blocks, ignore_index=True))
    indicators_path = OUTPUT_DIR / f"primary_survey_indicators_{SURVEY_YEAR}.csv"
    indicators_combined.to_csv(indicators_path, index=False)
    logger.info(f"Wrote {len(indicators_combined)} indicator rows to {indicators_path}")
    total_rows += len(indicators_combined)

    questions_combined = suppress_small_cells(pd.concat(question_blocks, ignore_index=True))
    questions_path = OUTPUT_DIR / f"primary_survey_questions_{SURVEY_YEAR}.csv"
    questions_combined.to_csv(questions_path, index=False)
    logger.info(f"Wrote {len(questions_combined)} question rows to {questions_path}")
    total_rows += len(questions_combined)

    return TaskResult(task_name="survey_compiled", rows_inserted=total_rows, success=True)
