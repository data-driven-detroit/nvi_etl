import json
from pathlib import Path
import pandas as pd

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


def roll_up_income_categories(df):
    return pd.concat(
        [
            df[
                [
                    "income_lt_10000",
                    "income_10000_to_14999",
                    "income_20000_to_24999",
                ]
            ].sum(axis=1),
            df[
                [
                    "income_25000_to_29999",
                    "income_30000_to_34999",
                    "income_35000_to_39999",
                    "income_40000_to_44999",
                    "income_45000_to_49999",
                    "income_50000_to_59999",
                    "income_60000_to_74999",
                ]
            ].sum(axis=1),
            df[
                [
                    "income_75000_to_99999",
                    "income_100000_to_124999",
                    "income_125000_to_149999",
                    "income_150000_to_199999",
                    "income_ge_200000",
                ]
            ].sum(axis=1),
        ],
        axis=1,
    )


def hierfindal(categories):
    ratios = categories.div(
        categories.sum(axis=1), axis=0
    )  # Calculate the percents
    return 1 - (ratios**2).sum(axis=1)  # 1 minus the sum of squares


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
        .assign(
            year="2023", 
        )
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

    # COMPILING CONTEXT INDICATORS
    def median_home_value_estimate(row):
        distribution = (
            ((0, 10_000), row["oo_value_dist_1"]),
            ((10_000, 14_999), row["oo_value_dist_2"]),
            ((15_000, 19_999), row["oo_value_dist_3"]),
            ((20_000, 24_999), row["oo_value_dist_4"]),
            ((25_000, 29_999), row["oo_value_dist_5"]),
            ((30_000, 34_999), row["oo_value_dist_6"]),
            ((35_000, 39_999), row["oo_value_dist_7"]),
            ((40_000, 49_999), row["oo_value_dist_8"]),
            ((50_000, 59_999), row["oo_value_dist_9"]),
            ((60_000, 69_999), row["oo_value_dist_10"]),
            ((70_000, 79_999), row["oo_value_dist_11"]),
            ((80_000, 89_999), row["oo_value_dist_12"]),
            ((90_000, 99_999), row["oo_value_dist_13"]),
            ((100_000, 124_999), row["oo_value_dist_14"]),
            ((125_000, 149_999), row["oo_value_dist_15"]),
            ((150_000, 174_999), row["oo_value_dist_16"]),
            ((175_000, 199_999), row["oo_value_dist_17"]),
            ((200_000, 249_999), row["oo_value_dist_18"]),
            ((250_000, 299_999), row["oo_value_dist_19"]),
            ((300_000, 399_999), row["oo_value_dist_20"]),
            ((400_000, 499_999), row["oo_value_dist_21"]),
            ((500_000, 749_999), row["oo_value_dist_22"]),
            ((750_000, 999_999), row["oo_value_dist_23"]),
            ((1_000_000, 1_499_999), row["oo_value_dist_24"]),
            ((1_500_000, 1_999_999), row["oo_value_dist_25"]),
            ((2_000_000, float('inf')), row["oo_value_dist_26"]),
        )

        return estimate_median_from_distribution(distribution)
