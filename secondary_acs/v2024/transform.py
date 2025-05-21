from pathlib import Path
import pandas as pd

from nvi_etl.geo_reference import pull_tracts_to_nvi_crosswalk, pin_location
from nvi_etl.reshape import elongate
from secondary_acs.v2024.aggregations import compile_indicators


WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023


def build_geography_groups():
    # On this one we want just these two files put together

    wide_file = pd.read_csv(WORKING_DIR / "input" / "nvi_2024_acs.csv")
    districts = (
        pull_tracts_to_nvi_crosswalk(2020, 2026)
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

    # Add city-wide too
    geographies.append(
        wide_file.query("geoid == '06000US2616322000'")
        .groupby("year")
        .agg(sum_cols)
        .assign(
            geography="Detroit",
            geo_type="citywide"
        )
        .reset_index()
    )

    return pd.concat(geographies)


def create_primary_indicators(geography_counts, logger):
    primary_indicators = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv", index_col=False)
    indicators = compile_indicators(primary_indicators, logger)

    wide_table = (
        geography_counts
        .astype({"geography": "str"})
        .assign(
            **indicators,
            location_id=lambda df: df.apply(pin_location, axis=1),
        )
    )

    wide_table.to_csv(WORKING_DIR / "output" / "acs_primary_indicators_wide.csv")

    stub_names = ["count", "universe", "percentage", "rate", "per", "dollars", "index"]

    necessary_columns = [
        col for col in wide_table
        if ((col.split("_")[0] in stub_names)
            or (col in [
                "location_id", 
                "year", 
                "indicator", 
                "geo_type", 
                "geography"
            ])
        )
    ]

    wide_trimmed = wide_table[necessary_columns]

    tall = elongate(wide_trimmed) # Review code in nvi_etl.reshape / pd.wide_to_long

    primary_tall = (
        tall
        .merge(primary_indicators, on=["indicator", "year"], how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .sort_values(["indicator_id", "location_id"])
    )

    primary_tall["year"] = 2024 # FIXME This will be dealt with when we move Secondary to Context
    primary_tall.to_csv(WORKING_DIR / "output" / "acs_primary_indicators_tall.csv", index=False)


def create_context_indicators(geography_counts, logger):
    context_indicators = pd.read_csv(WORKING_DIR / "conf" / "context_indicator_ids.csv", index_col=False)
    indicators = compile_indicators(context_indicators, logger)

    wide_table = (
        geography_counts
        .astype({"geography": "str"})
        .assign(
            **indicators,
            location_id=lambda df: df.apply(pin_location, axis=1),
        )
    )

    wide_table.to_csv(WORKING_DIR / "output" / "acs_secondary_indicators_wide.csv")

    stub_names = ["count", "universe", "percentage", "rate", "per", "dollars", "index"]

    necessary_columns = [
        col for col in wide_table
        if ((col.split("_")[0] in stub_names)
            or (col in [
                "location_id", 
                "year", 
                "indicator", 
                "geo_type", 
                "geography"
            ])
        )
    ]

    wide_trimmed = wide_table[necessary_columns]

    tall = elongate(wide_trimmed) # Review code in nvi_etl.reshape / pd.wide_to_long

    context_tall = (
        tall
        .merge(context_indicators, on=["indicator", "year"], how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .sort_values(["indicator_id", "location_id"])
    )

    context_tall.to_csv(WORKING_DIR / "output" / "acs_context_indicators_tall.csv", index=False)


def transform(logger):
    logger.info("Aggregating tract-level ACS data to 2026 Council Districts")

    geography_counts = build_geography_groups()
    create_primary_indicators(geography_counts, logger)
    create_context_indicators(geography_counts, logger)



if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform(logger)