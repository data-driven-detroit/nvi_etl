"""ACS (American Community Survey) data via d3census -- primary ACS pipeline."""

import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import CONF_DIR
from nvi_etl.registry import task, TaskResult
from nvi_etl.reshape import elongate
from nvi_etl.aggregations import compile_indicators
from nvi_etl.geo import pull_tracts_to_nvi_crosswalk, pin_location
from nvi_etl.schema import (
    NVIValueTable, NVIContextValueTable,
    SURVEY_VALUES_TABLE, CONTEXT_VALUES_TABLE,
)

YEARS = [2013, 2018, 2023]


def _extract_acs(logger):
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

    logger.info(f"Pulling all ACS data for {YEARS[-1]}")
    edition = create_edition("acs5", YEARS[-1])
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
    ).assign(year=YEARS[-1])

    comparisons = [acs_present]
    for year in YEARS:
        logger.info(f"Pulling overtime data for {year}")
        edition = create_edition("acs5", year)
        profile = build_profile(
            [DETROIT, WAYNE_TRACTS],
            OVERTIME_INDICATORS,
            edition,
        ).assign(year=year)
        comparisons.append(profile)

    return pd.concat(comparisons)


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

    return (
        tall
        .merge(indicators_df, on=["indicator", "year"], how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .sort_values(["indicator_id", "location_id"])
    )


@task("acs", phase=1, description="ACS Census data via d3census (primary pipeline)")
def run(source: Engine, target: Engine) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    total_rows = 0

    # Extract
    wide_file = _extract_acs(logger)

    # Aggregate to geographies
    geography_counts = _build_geography_groups(wide_file, source)

    # Primary indicators
    primary_tall = _build_indicator_tall(
        geography_counts,
        CONF_DIR / "ipds" / "primary_indicator_ids.csv",  # shared indicator IDs
        logger,
    )
    primary_tall["year"] = 2024
    primary_tall["value_type_id"] = 1

    validated_primary = NVIValueTable.validate(primary_tall)
    validated_primary.to_sql(SURVEY_VALUES_TABLE, target, schema="nvi", index=False, if_exists="append")
    total_rows += len(validated_primary)

    # Context indicators
    context_tall = _build_indicator_tall(
        geography_counts,
        CONF_DIR / "ipds" / "context_indicator_ids.csv",  # shared indicator IDs
        logger,
    )

    validated_context = NVIContextValueTable.validate(context_tall)
    validated_context.to_sql(CONTEXT_VALUES_TABLE, target, schema="nvi", index=False, if_exists="append")
    total_rows += len(validated_context)

    return TaskResult(task_name="acs", rows_inserted=total_rows, success=True)
