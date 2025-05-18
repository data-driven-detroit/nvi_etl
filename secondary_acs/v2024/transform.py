from pathlib import Path
import pandas as pd

from nvi_etl.geo_reference import pull_tracts_to_nvi_crosswalk, pin_location
from secondary_acs.v2024.aggregations import compile_indicators


WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023


def transform(logger):
    logger.info("Aggregating tract-level ACS data to 2026 Council Districts")

    districts = pull_tracts_to_nvi_crosswalk(2020, 2026)
    wide_file = pd.read_csv(WORKING_DIR / "input" / "nvi_2024_acs.csv")

    sum_cols = {col: "sum" for col in wide_file.columns if col not in {"name", "year", "geoid"}}

    primary_indicators = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv", index_col=False)
    indicators = compile_indicators(primary_indicators, logger)

    # NVI Zones

    zones_wide = (
        districts.rename(
            columns={
                "tract_geoid": "geoid",
                "zone_name": "geography",
            }
        )
        .astype({"geoid": "str"})
        .merge(wide_file, on="geoid", how="left")
        .groupby(["geography", "year"])
        .agg(sum_cols)
        .reset_index()
        .astype({"geography": "str"})
        .assign(
            **indicators,
            geo_type="zone",
            location_id=lambda df: df.apply(pin_location, axis=1),
        )
    )

    # Detroit Council Districts

    districts_wide = (
        districts.rename(
            columns={
                "tract_geoid": "geoid",
                "district_number": "geography",
            }
        )
        .astype({"geoid": "str"})
        .merge(wide_file, on="geoid", how="left")
        .groupby(["geography", "year"])
        .agg(sum_cols)
        .reset_index()
        .astype({"geography": "str"})
        .assign(
            **indicators,
            # index_hierfindal=lambda df: hierfindal(roll_up_income_categories(df)),
            geo_type="district",
            location_id=lambda df: df.apply(pin_location, axis=1),
        )
    )

    # City-wide

    citywide_wide = (
        wide_file.query("geoid == '06000US2616322000'")
        .groupby("year")
        .agg(sum_cols)
        .reset_index()
        .assign(  # Assign the district-level aggs with the functions above
            **indicators,
            # hierfindal=lambda df: hierfindal(roll_up_income_categories(df)),
            geo_type="citywide",
            geography="Detroit",
            location_id=1,
        )
    )


    wide_collected = pd.concat([citywide_wide, districts_wide, zones_wide])
    wide_collected.to_csv(WORKING_DIR / "output" / "acs_primary_indicators_wide.csv")

    stub_names = ["count", "universe", "percentage", "rate", "per", "dollars", "index"]

    wide_trimmed = wide_collected[[
        col for col in wide_collected
        if (
            (col.split("_")[0] in stub_names)
            or (col in ["location_id", "year", "indicator", "geo_type", "geography"])
        )
    ]]

    tall = (
        pd.wide_to_long(
            wide_trimmed,
            stubnames=["count", "universe", "percentage", "rate", "per", "dollars", "index"],
            i=["location_id", "year"],
            j="indicator",
            sep="_",
            suffix=".*",
        )
        .reset_index()
        .rename(columns={"per": "rate_per"})
    )

    primary_tall = (
        tall
        .merge(primary_indicators, on=["indicator", "year"], how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
    )

    primary_tall["year"] = 2024 # FIXME This will be dealth with when we move Secondary to Context
    primary_tall.to_csv(WORKING_DIR / "output" / "acs_primary_indicators_tall.csv", index=False)


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform(logger)