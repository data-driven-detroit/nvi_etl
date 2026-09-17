"""Response-level indicator evaluation.

For each geocoded survey response, evaluates every indicator defined in
the answer key and outputs a wide-format CSV: one row per respondent,
with demographic columns and a boolean column per indicator.
"""

import os
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import DUA_FOLDER
from nvi_etl.registry import task, TaskResult
from nvi_etl.tasks.primary_survey import (
    SURVEY_YEAR,
    SURVEY_CONF,
    combine_survey_and_geocoded,
    read_answer_key,
)
from nvi_etl.tasks.primary_survey_cdo import pull_indicator_names

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "survey" / "output"

DEMOGRAPHIC_COLS = [
    "Education_Level",
    "Household_Annual_Income_Before_Taxes",
    "Own_Or_Rent",
    "Age",
]

RACE_COL_SUFFIX = ":Race_And_Ethnicities"


def _snake(name):
    """Convert an indicator name to a snake_case column name."""
    s = re.sub(r"[^\w\s]", "", name)
    s = re.sub(r"\s+", "_", s.strip())
    return s.lower()


def _evaluate_single(survey_data, datadictionary, indicator_id, survey_date):
    """Evaluate a SINGLE/GROUPED-SINGLE indicator per response.

    Returns a Series indexed by the survey_data index with boolean values.
    Respondents outside the universe get NaN.
    """
    indicator_rows = datadictionary[
        (datadictionary["indicator_db_id"] == indicator_id)
        & (datadictionary["start_date"] <= survey_date)
        & (datadictionary["end_date"] > survey_date)
    ]
    relevant_columns = indicator_rows["full_column"].drop_duplicates()
    indicator_meta = indicator_rows.iloc[0]

    q = indicator_meta["universe_query"]
    universe = survey_data if q == "@ALL" else survey_data.query(q)
    universe = universe.dropna(subset=relevant_columns, how="all")

    accepted_values = {
        column: list(answers[answers["indicator_include"]]["survey_code"])
        for column, answers in indicator_rows.groupby("full_column")
    }
    index_include = universe[relevant_columns].isin(accepted_values)

    if indicator_meta["and_or"] == "AND":
        included = index_include[relevant_columns].all(axis=1)
    elif indicator_meta["and_or"] == "OR":
        included = index_include[relevant_columns].any(axis=1)
    else:
        raise ValueError(f"{indicator_meta['and_or']} is not a valid value for 'and_or'")

    result = pd.Series(pd.NA, index=survey_data.index, dtype=pd.BooleanDtype())
    result.loc[included.index] = included.astype(pd.BooleanDtype())
    return result


def _evaluate_multi(survey_data, datadictionary, indicator_id, survey_date):
    """Evaluate a MULTI-SELECT indicator per response."""
    indicator_rows = datadictionary[
        (datadictionary["indicator_db_id"] == indicator_id)
        & datadictionary["indicator_include"]
        & (datadictionary["start_date"] <= survey_date)
        & (datadictionary["end_date"] > survey_date)
    ]
    indicator_meta = indicator_rows.iloc[0]
    relevant_columns = indicator_rows["full_column"].drop_duplicates()

    q = indicator_meta["universe_query"]
    universe = survey_data if q == "@ALL" else survey_data.query(q)

    present = ~universe[indicator_rows["full_column"]].isna()

    if indicator_meta["and_or"] == "AND":
        included = present[relevant_columns].all(axis=1)
    elif indicator_meta["and_or"] == "OR":
        included = present[relevant_columns].any(axis=1)
    else:
        raise ValueError(f"{indicator_meta['and_or']} is not a valid value for 'and_or'")

    result = pd.Series(pd.NA, index=survey_data.index, dtype=pd.BooleanDtype())
    result.loc[included.index] = included.astype(pd.BooleanDtype())
    return result


_EVALUATORS = {
    "SINGLE": _evaluate_single,
    "GROUPED-SINGLE": _evaluate_single,
    "MULTI-SELECT": _evaluate_multi,
}


@task("survey_response_indicators", phase=2, description="Response-level indicator flags from the answer key")
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

    logger.info("Opening survey files for response-level indicators")
    frame = pd.read_csv(survey_csv, low_memory=False).rename(
        columns={"Response ID": "response_id"}
    )
    geoframe = gpd.read_file(geocoded_shp).rename(
        columns={"Response_I": "response_id"}
    )
    datadictionary = read_answer_key()

    geocoded = combine_survey_and_geocoded(frame, geoframe)
    survey_date = pd.Timestamp(year=SURVEY_YEAR, month=1, day=1)

    # -- Indicator names (optional) -----------------------------------------
    nvi_db = kwargs.get("nvi_db")
    indicator_names = None
    if nvi_db is not None:
        logger.info("Pulling indicator names from NVI database")
        indicator_names = pull_indicator_names(nvi_db)
        name_lookup = dict(zip(indicator_names["indicator_db_id"], indicator_names["indicator_name"]))

    # -- Demographics -------------------------------------------------------
    race_cols = [c for c in geocoded.columns if c.endswith(RACE_COL_SUFFIX)]
    demo_cols = [c for c in DEMOGRAPHIC_COLS if c in geocoded.columns]

    output = geocoded[["response_id"] + demo_cols + race_cols].copy()

    # -- Evaluate each indicator --------------------------------------------
    indicators = (
        datadictionary.dropna(subset="indicator_db_id")
        .drop_duplicates(subset=["indicator_db_id", "response_type"])
    )
    indicators = indicators[
        (indicators["start_date"] <= survey_date)
        & (indicators["end_date"] > survey_date)
    ]

    errors = []
    for _, indicator in indicators.iterrows():
        iid = int(indicator["indicator_db_id"])
        evaluator = _EVALUATORS.get(indicator["response_type"])
        if evaluator is None:
            errors.append((iid, f"{indicator['response_type']} is invalid"))
            continue

        if indicator_names is not None and iid in name_lookup:
            col_name = _snake(name_lookup[iid])
        else:
            col_name = f"var{iid}"

        try:
            output[col_name] = evaluator(geocoded, datadictionary, iid, survey_date)
        except KeyError as e:
            errors.append((iid, f"Column not found in survey data: {e}"))

    if errors:
        for err_id, err_msg in errors:
            logger.warning(f"Indicator {err_id}: {err_msg}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"survey_response_indicators_{SURVEY_YEAR}.parquet"
    output.to_parquet(output_path, index=False)

    logger.info(f"Wrote {len(output)} rows ({output.shape[1]} cols) to {output_path}")

    return TaskResult(
        task_name="survey_response_indicators",
        rows_inserted=len(output),
        success=True,
    )
