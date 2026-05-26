"""ACS (American Community Survey) data via d3census -- primary ACS pipeline.

Pulls ACS 5-year estimates for all indicators at the current edition,
plus overtime indicators (population, etc.) at multiple historical editions.

Survey year 2024: ACS5 2023 data, overtime years [2013, 2018, 2023]
Survey year 2025: ACS5 2024 data, overtime years [2014, 2019, 2024]
"""

import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import CONF_DIR, CENSUS_API_KEY
from nvi_etl.registry import task, TaskResult
from nvi_etl.reshape import elongate
from nvi_etl.aggregations import compile_indicators
from nvi_etl.geo import pull_tracts_to_nvi_crosswalk, pin_location
from nvi_etl.upsert import upsert_values, upsert_context_values

ACS_CONF = CONF_DIR.parent / "acs" / "conf"

# Each survey year maps to an ACS edition and overtime comparison years
SURVEY_YEARS = {
    2024: {
        "acs_edition": 2023,
        "overtime_years": [2013, 2018, 2023],
    },
    2025: {
        "acs_edition": 2024,
        "overtime_years": [2014, 2019, 2024],
    },
}


def _extract_acs(acs_edition, overtime_years, logger):
    """Pull ACS data from Census API via d3census."""
    from d3census import create_geography, create_edition, build_profile
    from nvi_etl.acs.variables import (
        AGE_DISTRIBUTION_VARIABLES,
        GROSS_RENT_DISTRIBUTION,
        RACE_ETHNICITY_VARIABLES,
        INCOME_DISTRIBUTION_VARIABLES,
        HOME_VALUE_DISTRIBUTION_VARIABLES,
        OTHER_INDICATORS,
        OVERTIME_INDICATORS,
    )

    DETROIT = create_geography(state="26", county="163", county_subdivision="22000")
    WAYNE_TRACTS = create_geography(state="26", county="163", tract="*")

    # Pull all indicators at the current edition
    logger.info(f"Pulling all ACS data for edition {acs_edition}")
    edition = create_edition("acs5", acs_edition)
    acs_present = build_profile(
        [DETROIT, WAYNE_TRACTS],
        [
            *AGE_DISTRIBUTION_VARIABLES,
            *GROSS_RENT_DISTRIBUTION,
            *RACE_ETHNICITY_VARIABLES,
            *INCOME_DISTRIBUTION_VARIABLES,
            *HOME_VALUE_DISTRIBUTION_VARIABLES,
            *OTHER_INDICATORS,
        ],
        edition,
        api_key=CENSUS_API_KEY,
    ).assign(year=acs_edition)

    # Pull overtime indicators at each comparison year
    comparisons = [acs_present]
    for year in overtime_years:
        logger.info(f"Pulling overtime data for {year}")
        edition = create_edition("acs5", year)
        profile = build_profile(
            [DETROIT, WAYNE_TRACTS],
            OVERTIME_INDICATORS,
            edition,
            api_key=CENSUS_API_KEY,
        ).assign(year=year)
        comparisons.append(profile)

    return pd.concat(comparisons).copy()


def _build_geography_groups(wide_file, source):
    """Aggregate tract-level data to zones, districts, and citywide."""
    districts = (
        pull_tracts_to_nvi_crosswalk(source, 2020, 2026)
        .rename(columns={"tract_geoid": "geoid"})
        .astype({"geoid": "str"})
    )

    sum_cols = {col: "sum" for col in wide_file.columns if col not in {"name", "year", "geoid"}}

    geographies = []
    for geo_type, agg in [("zone", "zone_name"), ("district", "district_number")]:
        geographies.append(
            districts
            .rename(columns={agg: "geography"})
            .merge(wide_file, on="geoid", how="left")
            .groupby(["geography", "year"])
            .agg(sum_cols)
            .assign(geo_type=geo_type)
            .reset_index()
        )

    geographies.append(
        wide_file.query("geoid == '06000US2616322000'")
        .groupby("year")
        .agg(sum_cols)
        .assign(geography="Detroit", geo_type="citywide")
        .reset_index()
    )

    return pd.concat(geographies)


def _build_indicator_tall(geography_counts, indicators_csv, logger):
    """Build tall-format indicators from wide geography counts."""
    indicators_df = pd.read_csv(indicators_csv, index_col=False)
    agg_funcs = compile_indicators(indicators_df, logger)

    wide_table = (
        geography_counts
        .astype({"geography": "str"})
        .assign(**agg_funcs, location_id=lambda df: df.apply(pin_location, axis=1))
    )

    stub_names = ["count", "universe", "percentage", "rate", "per", "dollars", "index"]
    necessary_columns = [
        col for col in wide_table
        if col.split("_")[0] in stub_names
        or col in ["location_id", "year", "indicator", "geo_type", "geography"]
    ]

    tall = elongate(wide_table[necessary_columns])

    merged = (
        tall
        .merge(indicators_df, on=["indicator", "year"], how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .sort_values(["indicator_id", "location_id"])
    )

    missing = merged["location_id"].isna()
    if missing.any():
        logger.warning(
            f"{missing.sum()} rows dropped: no matching data for some "
            f"indicator/year combinations in {indicators_csv.name}"
        )
        merged = merged.dropna(subset=["location_id"])

    return merged


@task("acs", phase=1, description="ACS Census data via d3census")
def run(source: Engine, target: Engine) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    total_rows = 0

    for survey_year, config in SURVEY_YEARS.items():
        acs_edition = config["acs_edition"]
        overtime_years = config["overtime_years"]

        logger.info(f"Processing ACS for survey year {survey_year} (ACS5 {acs_edition})")

        # Extract
        wide_file = _extract_acs(acs_edition, overtime_years, logger)

        # Aggregate to geographies
        geography_counts = _build_geography_groups(wide_file, source)

        # Primary indicators
        primary_tall = _build_indicator_tall(
            geography_counts,
            ACS_CONF / "primary_indicator_ids.csv",
            logger,
        )
        primary_tall["survey_id"] = 1
        primary_tall["value_type_id"] = 1

        total_rows += upsert_values(target, primary_tall, schema="nvi")

        # Context indicators
        context_tall = _build_indicator_tall(
            geography_counts,
            ACS_CONF / "context_indicator_ids.csv",
            logger,
        )

        total_rows += upsert_context_values(target, context_tall, schema="nvi")

    return TaskResult(task_name="acs", rows_inserted=total_rows, success=True)
