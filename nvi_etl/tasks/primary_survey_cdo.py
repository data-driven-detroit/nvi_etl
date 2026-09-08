"""Primary NVI survey ETL task -- CDO boundary aggregation.

Reads the same survey CSV + geocoded shapefile as primary_survey, but
performs spatial joins to CDO service areas instead of council districts
and zones.  Output is a single human-readable CSV with small-cell
suppression (counts below 6 replaced with '*').

Also produces citywide aggregation rows (from the full, unfiltered frame)
so downstream workbook generation can compare CDO data to citywide.
"""

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import DUA_FOLDER
from nvi_etl.registry import task, TaskResult
from nvi_etl.geo import pull_cdo_boundaries
from nvi_etl.tasks.primary_survey import (
    SURVEY_YEAR,
    SURVEY_CONF,
    combine_survey_and_geocoded,
    create_indicator_rows,
    create_question_rows,
)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "survey" / "output"

SUPPRESSION_THRESHOLD = 6


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


def suppress_small_cells(df):
    """Apply small-cell suppression with complementary suppression.

    When only one answer option within a question-location group falls
    below the threshold, the next-smallest option is also suppressed so
    the hidden value cannot be reconstructed from universe minus the
    visible counts.
    """
    value_cols = {"answer", "count", "universe", "percentage"}
    group_cols = [c for c in df.columns if c not in value_cols]

    for col in ("count", "universe", "percentage"):
        df[col] = df[col].astype(object)

    suppress_mask = pd.Series(False, index=df.index)

    for _, group in df.groupby(group_cols, dropna=False):
        counts = pd.to_numeric(group["count"], errors="coerce")
        small = counts < SUPPRESSION_THRESHOLD
        n_small = small.sum()

        if n_small == 0:
            continue

        suppress_mask.loc[small[small].index] = True

        if n_small == 1:
            not_small = ~small & counts.notna()
            if not_small.any():
                suppress_mask.loc[counts[not_small].idxmin()] = True

    df.loc[suppress_mask, ["count", "universe", "percentage"]] = "*"
    return df


def _build_indicator_block(frame, datadictionary, survey_date, summaries, geo_label_col, indicator_names=None):
    """Run indicator aggregation and join text fields from the datadictionary."""
    indicators, errors = create_indicator_rows(
        frame, datadictionary, survey_date, summaries
    )

    indicator_text = (
        datadictionary.dropna(subset="indicator_db_id")
        .drop_duplicates(subset="indicator_db_id")
        [["indicator_db_id", "topic_text", "question_text"]]
    )
    output = (
        indicators.rename(columns={"location_id": geo_label_col})
        .merge(
            indicator_text,
            left_on="indicator_id",
            right_on="indicator_db_id",
            how="left",
        )
        .assign(answer="[INDICATOR]", value_type="indicator")
        .rename(columns={"indicator_id": "indicator_db_id"})
    )
    if indicator_names is not None:
        output = output.merge(indicator_names, on="indicator_db_id", how="left")
    return output, errors


def _build_question_block(frame, datadictionary, survey_date, summaries, geo_label_col, indicator_names=None):
    """Run question aggregation."""
    table = create_question_rows(frame, datadictionary, survey_date, summaries)
    table = table.rename(columns={"location": geo_label_col}).assign(value_type="question")
    if indicator_names is not None:
        table = table.merge(
            indicator_names,
            left_on="indicator_db_id",
            right_on="indicator_db_id",
            how="left",
        )
    return table


def pull_indicator_names(engine):
    """Pull indicator id-to-name mapping from the NVI application database."""
    return pd.read_sql(
        "SELECT id AS indicator_db_id, name AS indicator_name FROM nvi.indicators",
        engine,
    )


@task("primary_survey_cdo", phase=2, description="Primary NVI survey -- CDO boundary aggregation")
def run(source: Engine, target: Engine, **kwargs) -> TaskResult:
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
    survey_date = pd.Timestamp(year=SURVEY_YEAR, month=1, day=1)

    # -- Indicator names from NVI app database (optional) -------------------
    nvi_db = kwargs.get("nvi_db")
    indicator_names = None
    if nvi_db is not None:
        logger.info("Pulling indicator names from NVI database")
        indicator_names = pull_indicator_names(nvi_db)

    # -- Citywide aggregation (full frame, no spatial filter) ---------------
    logger.info("Running citywide aggregation for comparison")
    citywide_frame = geocoded.assign(citywide="citywide")

    cw_indicators, cw_errors = _build_indicator_block(
        citywide_frame, datadictionary, survey_date, ["citywide"], "organization_name",
        indicator_names=indicator_names,
    )
    cw_questions = _build_question_block(
        citywide_frame, datadictionary, survey_date, ["citywide"], "organization_name",
        indicator_names=indicator_names,
    )
    if cw_errors:
        for err_id, err_msg in cw_errors:
            logger.warning(f"Citywide indicator {err_id}: {err_msg}")

    # -- CDO aggregation (spatially filtered frame) -------------------------
    logger.info("Pulling CDO boundaries for aggregation")
    cdo_boundaries = pull_cdo_boundaries(source)
    cdo_frame = add_cdo_boundaries(geocoded, cdo_boundaries)

    logger.info("Creating CDO indicator rows")
    cdo_indicators, cdo_errors = _build_indicator_block(
        cdo_frame, datadictionary, survey_date, ["organization_name"], "organization_name",
        indicator_names=indicator_names,
    )
    if cdo_errors:
        for err_id, err_msg in cdo_errors:
            logger.warning(f"CDO indicator {err_id}: {err_msg}")

    logger.info("Creating CDO question rows")
    cdo_questions = _build_question_block(
        cdo_frame, datadictionary, survey_date, ["organization_name"], "organization_name",
        indicator_names=indicator_names,
    )

    # -- Combine all rows ---------------------------------------------------
    shared_columns = [
        "organization_name", "indicator_name", "topic_text", "question_text",
        "answer", "count", "universe", "percentage", "value_type",
    ]
    if indicator_names is None:
        shared_columns.remove("indicator_name")
    combined = pd.concat([
        cdo_indicators[shared_columns],
        cdo_questions[shared_columns],
        cw_indicators[shared_columns],
        cw_questions[shared_columns],
    ], ignore_index=True)

    # Merge location IDs for CDO rows
    combined = combined.merge(
        location_dictionary,
        left_on="organization_name",
        right_on="location",
        how="left",
    ).drop(columns="location", errors="ignore")

    # Suppress small cells
    combined = suppress_small_cells(combined)

    # Write single CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"primary_survey_cdo_{SURVEY_YEAR}.csv"
    combined.to_csv(output_path, index=False)

    logger.info(f"Wrote {len(combined)} rows to {output_path}")

    return TaskResult(task_name="primary_survey_cdo", rows_inserted=len(combined), success=True)
