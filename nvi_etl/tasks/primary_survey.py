"""Primary NVI survey ETL task.

Reads survey CSV + geocoded shapefile, performs spatial joins to districts
and zones, aggregates indicators and questions, and loads to the value table.
"""

import os
from datetime import date

import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import CONF_DIR, DUA_FOLDER
from nvi_etl.db import get_engine
from nvi_etl.registry import task, TaskResult
from nvi_etl.geo import pull_council_districts, pull_zones
from nvi_etl.schema import SURVEY_VALUES_TABLE

SURVEY_YEAR = 2025
SURVEY_CONF = CONF_DIR.parent / "survey" / "conf"

VALUE_COLUMNS = [
    "year", "count", "universe", "percentage", "rate", "rate_per",
    "dollars", "indicator_id", "location_id", "survey_id",
    "survey_question_id", "survey_question_option_id", "value_type_id", "index",
]

OUTPUT_COLUMN_ORDER = [
    "location", "location_id", "summary_level", "full_column",
    "response_type", "db_question_code", "indicator_db_id",
    "topic_text", "question_text", "indicator_include", "universe_include",
    "survey_code", "db_answer_code", "answer", "count", "universe", "percentage",
]


def combine_survey_and_geocoded(frame, geoframe):
    return gpd.GeoDataFrame(
        frame.merge(geoframe[["response_id", "geometry"]], on="response_id")
    )


def add_districts_and_zones(geocoded, districts, zones):
    return (
        geocoded.to_crs(2898)
        .sjoin(districts[["geometry", "district_number"]], how="left", predicate="within")
        .drop(columns="index_right")
        .sjoin(zones[["geometry", "zone_id"]], how="left", predicate="within")
        .assign(citywide="citywide")
    )


def base_counts(frame):
    tables = []
    for summary in ("citywide", "district_number", "zone_id"):
        tables.append(
            frame.groupby(summary, dropna=False).size().to_frame().reset_index()
            .rename(columns={summary: "location"})
            .fillna({"location": "[NO ADDRESS PROVIDED]"})
        )
    return (
        pd.concat(tables)
        .drop_duplicates(subset="location", keep="last")
        .reset_index(drop=True)
    )


def append_universe_and_percentages(table):
    included = table["universe_include"]
    total = table[included].groupby("location")["count"].transform("sum")
    table["universe"] = total
    table.loc[included, "percentage"] = table.loc[included, "count"] / total
    return table


def aggregate_question(frame, column_name, summary, labels):
    table = frame[[column_name, summary]].value_counts(dropna=False).reset_index(summary)
    reference = labels.iloc[0]
    table.index = table.index.astype(pd.Int64Dtype())

    table = (
        table.join(labels, how="outer")
        .sort_values([summary, column_name])
        .reset_index(names="survey_code")
        .rename(columns={summary: "location"})
        .fillna({
            "topic_text": reference["topic_text"],
            "question_text": reference["question_text"],
            "full_column": reference["full_column"],
            "answer": "[SKIPPED]",
            "db_question_code": reference["db_question_code"],
            "indicator_include": False, "universe_include": False,
            "indicator_db_id": reference["indicator_db_id"],
            "location": "[NO ADDRESS PROVIDED]",
        })
        .astype({
            "db_question_code": pd.Int64Dtype(), "db_answer_code": pd.Int64Dtype(),
            "indicator_db_id": pd.Int64Dtype(),
            "universe_include": bool, "indicator_include": bool,
        })
        .assign(summary_level=summary)
    )
    return append_universe_and_percentages(table)


def roll_up_single(frame, groupdictionary, survey_date, summary):
    result = []
    for _, column in groupdictionary.drop_duplicates(subset="full_column").iterrows():
        if (column["start_date"] >= survey_date) or (column["end_date"] < survey_date) or (not column["tabulate"]):
            continue
        column_name = column["full_column"]
        labels = groupdictionary[groupdictionary["full_column"] == column_name][[
            "survey_question_topic_id", "topic_text", "question_text",
            "survey_code", "answer", "full_column", "response_type",
            "db_question_code", "db_answer_code", "indicator_include",
            "universe_include", "indicator_db_id",
        ]].set_index("survey_code")
        result.append(aggregate_question(frame, column_name, summary, labels))
    return pd.concat(result) if result else pd.DataFrame()


def roll_up_multiselect(frame, groupdatadictionary, survey_date, summary):
    groups = frame.groupby(summary)
    universe = groups.size().rename("universe").reset_index()
    labels = groupdatadictionary[
        (groupdatadictionary["start_date"] <= survey_date)
        & (groupdatadictionary["end_date"] > survey_date)
        & groupdatadictionary["tabulate"]
    ]
    return (
        groups[labels["full_column"]].count()
        .melt(ignore_index=False, var_name="full_column").reset_index()
        .merge(labels[[
            "full_column", "db_question_code", "db_answer_code", "and_or",
            "answer", "end_date", "group", "indicator_db_id",
            "indicator_include", "question_text", "response_type",
            "site_category", "start_date", "suppress_value", "survey_code",
            "survey_question_topic_id", "tabulate", "topic_text",
            "universe_include", "universe_query",
        ]], on="full_column")
        .merge(universe, on=summary)
        .rename(columns={"value": "count", summary: "location"})
        .assign(
            percentage=lambda f: (100 * f["count"] / f["universe"]).round(2),
            summary_level=summary,
        )
        .astype({"db_question_code": pd.Int64Dtype(), "db_answer_code": pd.Int64Dtype()})
    )


def create_question_rows(frame, datadictionary, survey_date, summaries):
    groups = datadictionary.groupby("group")
    tables = []
    for _, group in groups:
        for summary in summaries:
            response_type = group.iloc[0]["response_type"]
            if response_type in {"YES-NO", "SINGLE", "GROUPED-SINGLE"}:
                table = roll_up_single(frame, group, survey_date, summary)
            elif response_type == "MULTI-SELECT":
                table = roll_up_multiselect(frame, group, survey_date, summary)
            else:
                raise ValueError(f"{response_type} not valid.")
            tables.append(table)
    return pd.concat(tables)


def compile_single_response_indicator(survey_data, datadictionary, indicator_id, group_var):
    indicator_rows = datadictionary[datadictionary["indicator_db_id"] == indicator_id]
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
    intermediate = pd.concat([index_include, universe[group_var]], axis=1)

    if indicator_meta["and_or"] == "AND":
        combo_strategy = lambda df: df[relevant_columns].all(axis=1)
    elif indicator_meta["and_or"] == "OR":
        combo_strategy = lambda df: df[relevant_columns].any(axis=1)
    else:
        raise ValueError(f"{indicator_meta['and_or']} is not a valid value for 'and_or'")

    return (
        intermediate.assign(included=combo_strategy)
        .groupby(group_var).aggregate(
            count=pd.NamedAgg(column="included", aggfunc=lambda c: c.sum()),
            universe=pd.NamedAgg(column="included", aggfunc=lambda c: c.count()),
            percentage=pd.NamedAgg(column="included", aggfunc=lambda c: c.sum() / c.count()),
        )
    )


def compile_multi_response_indicator(survey_data, datadictionary, indicator_id, group_var):
    indicator_rows = datadictionary[
        (datadictionary["indicator_db_id"] == indicator_id) & datadictionary["indicator_include"]
    ]
    indicator_meta = indicator_rows.iloc[0]
    relevant_columns = indicator_rows["full_column"].drop_duplicates()

    q = indicator_meta["universe_query"]
    universe = survey_data if q == "@ALL" else survey_data.query(q)

    labeled = pd.concat([universe[group_var], ~universe[indicator_rows["full_column"]].isna()], axis=1)

    if indicator_meta["and_or"] == "AND":
        combo_strategy = lambda df: df[relevant_columns].all(axis=1)
    elif indicator_meta["and_or"] == "OR":
        combo_strategy = lambda df: df[relevant_columns].any(axis=1)
    else:
        raise ValueError(f"{indicator_meta['and_or']} is not a valid value for 'and_or'")

    return (
        labeled.assign(included=combo_strategy)
        .groupby(group_var).aggregate(
            count=pd.NamedAgg(column="included", aggfunc=lambda c: c.sum()),
            universe=pd.NamedAgg(column="included", aggfunc=lambda c: c.count()),
            percentage=pd.NamedAgg(column="included", aggfunc=lambda c: c.sum() / c.count()),
        )
    )


INDICATOR_COMPILERS = {
    "SINGLE": compile_single_response_indicator,
    "GROUPED-SINGLE": compile_single_response_indicator,
    "MULTI-SELECT": compile_multi_response_indicator,
}


def create_indicator_rows(frame, datadictionary, survey_date, summaries):
    indicators = (
        datadictionary.dropna(subset="indicator_db_id")
        .drop_duplicates(subset=["indicator_db_id", "response_type"])
    )
    indicators = indicators[
        (indicators["start_date"] <= survey_date) & (indicators["end_date"] > survey_date)
    ]

    result = []
    errors = []
    for _, indicator in indicators.iterrows():
        compiler = INDICATOR_COMPILERS.get(indicator["response_type"])
        if compiler is None:
            errors.append((indicator["indicator_db_id"], f"{indicator['response_type']} is invalid"))
            continue
        try:
            result.append(pd.concat([
                compiler(frame, datadictionary, indicator["indicator_db_id"], agg)
                .reset_index().rename(columns={agg: "location_id"})
                .assign(indicator_id=indicator["indicator_db_id"], year=SURVEY_YEAR)
                for agg in summaries
            ]))
        except KeyError:
            errors.append((indicator["indicator_db_id"], "Universe returned no rows."))

    return pd.concat(result).assign(value_type_id=1), errors


@task("primary_survey", phase=2, description="Primary NVI survey -- indicators and questions")
def run(source: Engine, target: Engine) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    # Read source files -- paths resolve via NVI_DUA_FOLDER env var
    survey_csv = os.environ.get(
        "NVI_SURVEY_CSV",
        str(DUA_FOLDER / "3_Projects" / "NVI" / "2025" / "nvi_survey_data_2025_20260226.csv"),
    )
    geocoded_shp = os.environ.get(
        "NVI_GEOCODED_SHP",
        str(DUA_FOLDER / "3_Projects" / "NVI" / "2025" / "Final Shapefiles" / "Final2025NVIDataset_cleaned_20260304.shp"),
    )

    logger.info("Opening survey files")
    frame = pd.read_csv(survey_csv, low_memory=False).rename(
        columns={"Response ID": "response_id"}
    )
    geoframe = gpd.read_file(geocoded_shp).rename(
        columns={"Response_I": "response_id"}
    )
    datadictionary = pd.read_excel(SURVEY_CONF / "nvi_answer_key_20260316.xlsx")
    location_dictionary = pd.read_excel(
        SURVEY_CONF / "locations_20260312.xlsx",
        dtype={"location": str, "universe_include": bool},
    )

    geocoded = combine_survey_and_geocoded(frame, geoframe)

    # Spatial join
    logger.info("Pulling down districts and zones for aggregation")
    districts = pull_council_districts(source, 2026)
    zones = pull_zones(source, 2026)
    complete_frame = add_districts_and_zones(geocoded, districts, zones)

    survey_date = pd.Timestamp(year=SURVEY_YEAR, month=1, day=1)
    summaries = ["citywide", "district_number", "zone_id"]
    total_rows = 0

    # Indicator aggregation
    indicators, errors = create_indicator_rows(
        complete_frame, datadictionary, survey_date, summaries
    )
    if errors:
        for err_id, err_msg in errors:
            logger.warning(f"Indicator {err_id}: {err_msg}")

    indicator_db = (
        indicators.rename(columns={"location_id": "location"})
        .merge(location_dictionary, on="location", how="left")
        .astype({"location_id": pd.Int64Dtype()})
        .assign(
            rate=pd.NA, rate_per=pd.NA, dollars=pd.NA,
            survey_id=1, index=pd.NA,
            survey_question_id=pd.NA, survey_question_option_id=pd.NA,
        )
        .dropna(subset=["location_id", "indicator_id"])
        .astype({
            "dollars": pd.Int64Dtype(), "rate": pd.Int64Dtype(),
            "rate_per": pd.Int64Dtype(), "index": pd.Int64Dtype(),
            "survey_question_id": pd.Int64Dtype(),
            "survey_question_option_id": pd.Int64Dtype(),
        })
    )[VALUE_COLUMNS]

    indicator_db.drop_duplicates(subset=[
        "indicator_id", "location_id", "survey_id",
        "survey_question_id", "survey_question_option_id",
    ]).to_sql(SURVEY_VALUES_TABLE, target, if_exists="append", index=False)
    total_rows += len(indicator_db)

    # Question aggregation
    logger.info("Creating question rows")
    table = create_question_rows(
        complete_frame, datadictionary, survey_date, summaries
    )
    table = (
        table.merge(location_dictionary, on="location", how="left")
        .astype({"location_id": pd.Int64Dtype(), "survey_code": pd.Int64Dtype()})
    )[OUTPUT_COLUMN_ORDER]

    question_db = (
        table.rename(columns={
            "db_question_code": "survey_question_id",
            "indicator_db_id": "indicator_id",
            "db_answer_code": "survey_question_option_id",
        })
        .assign(
            year=SURVEY_YEAR, rate=pd.NA, rate_per=pd.NA,
            dollars=pd.NA, survey_id=1, value_type_id=2, index=pd.NA,
        )
        .dropna(subset=["location_id", "indicator_id"])
        .astype({
            "dollars": pd.Int64Dtype(), "rate": pd.Int64Dtype(),
            "rate_per": pd.Int64Dtype(), "index": pd.Int64Dtype(),
        })
    )[VALUE_COLUMNS]

    question_db.drop_duplicates(subset=[
        "indicator_id", "location_id", "survey_id",
        "survey_question_id", "survey_question_option_id",
    ]).to_sql(SURVEY_VALUES_TABLE, target, if_exists="append", index=False)
    total_rows += len(question_db)

    return TaskResult(task_name="primary_survey", rows_inserted=total_rows, success=True)
