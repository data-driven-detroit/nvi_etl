import json
from pathlib import Path
import pandas as pd
from datetime import date

from nvi_etl.geo_reference import pull_tracts_to_nvi_crosswalk, pin_location
from nvi_etl.utilities import estimate_median_from_distribution


WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023

primary_indicator_meta = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv", index_col=False)
context_indicator_meta = pd.read_csv(WORKING_DIR / "conf" / "context_indicator_ids.csv", index_col=False)

# These can be combined until the final step
all_indicators = pd.concat([
    primary_indicator_meta,
    context_indicator_meta,
])


sum_cols = {}
for indicator, indicator_type in all_indicators[["indicator", "indicator_type"]].values:
    if indicator_type in {"count", "percentage", "rate"}:
        sum_cols[f"count_{indicator}"] = "sum"

    if indicator_type == "percentage":
        sum_cols[f"universe_{indicator}"] = "sum"


pct_aggregators = {
    f"percentage_{indicator}": lambda df, indicator=indicator: (
        df[f"count_{indicator}"] * 100
        / df[f"universe_{indicator}"]
    ) for indicator, indicator_type in all_indicators[["indicator", "indicator_type"]].values
    if indicator_type == "percentage"
}


def transform(logger):
    logger.info("Aggregating tract-level ACS data to 2026 Council Districts")

    districts = pull_tracts_to_nvi_crosswalk(2020, 2026)
    wide_file = pd.read_parquet(WORKING_DIR / "input" / "nvi_2024_acs.parquet.gzip")

    # COMPILING 'REGULAR' INDICATORS
    # TODO Read this dynamically off of the locations file

    # Tract-level
    districts_wide = (
        districts.rename(
            columns={
                "tract_geoid": "geoid",
                "district_number": "geography",
            }
        )
        .astype({"geoid": "str"})
        .merge(wide_file, on="geoid", how="left")
        .groupby("geography")
        .agg(sum_cols)
        .reset_index()
        .astype({"geography": "str"})
        .assign(
            **pct_aggregators,
            # index_hierfindal=lambda df: hierfindal(roll_up_income_categories(df)),
            geo_type="district",
            location_id=lambda df: df.apply(pin_location, axis=1),
        )
    )

    assert len(districts_wide) == 7


    # NVI Zones

    zones_wide = (
        districts.rename(
            columns={
                "tract_geoid": "geoid",
                "district_number": "district",
                "zone_name": "geography",
            }
        )
        .astype({"geoid": "str"})
        .merge(wide_file, on="geoid", how="left")
        .groupby("geography")
        .agg(sum_cols)
        .reset_index()
        .astype({"geography": "str"})
        .assign(
            **pct_aggregators,
            # hierfindal=lambda df: hierfindal(roll_up_income_categories(df)),
            geo_type="zone",
            location_id=lambda df: df.apply(pin_location, axis=1),
        )
    )

    assert len(zones_wide) == 22

    # City-wide

    citywide_wide = (
        wide_file.query("geoid == '06000US2616322000'")
        .assign(district=1)
        .groupby("district")
        .agg(sum_cols)
        .assign(  # Assign the district-level aggs with the functions above
            **pct_aggregators,
            # hierfindal=lambda df: hierfindal(roll_up_income_categories(df)),
            geo_type="citywide",
            geography="Detroit",
            location_id=1,
        )
    )

    assert len(citywide_wide) == 1

    wide_collected = pd.concat([citywide_wide, districts_wide, zones_wide])

    tall = (
        pd.wide_to_long(
            wide_collected,
            stubnames=["count", "universe", "percentage", "rate", "per", "dollars", "index"],
            i=["location_id"],
            j="indicator",
            sep="_",
            suffix=".*",
        )
        .reset_index()
        .rename(columns={"per": "rate_per"})
    )

    primary_tall = (
        tall
        .merge(primary_indicator_meta, on="indicator", how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
    )
    primary_tall.to_csv(WORKING_DIR / "output" / "acs_primary_indicators_tall.csv", index=False)


    context_tall = (
        tall
        .merge(context_indicator_meta, on="indicator", how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
    )

    context_tall.to_csv(WORKING_DIR / "output" / "acs_context_indicators_tall.csv", index=False)
