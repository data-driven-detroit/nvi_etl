import json
from pathlib import Path
import pandas as pd

from nvi_etl import liquefy
from nvi_etl.geo_reference import pull_tracts_to_nvi_crosswalk


WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023


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


def pct_hs_diploma_ged_plus(df):
    return (df["population_ge_25_with_post_hs_diploma_ged"]) / df[
        "total_population_ge_25"
    ] * 100


def pct_post_secondary(df):
    return (
        df["population_ge_25_with_post_sec_degree"]
        / df["total_population_ge_25"]
    ) * 100


def pct_oo_spending_lt_30(df):
    return 100 * df["oo_hh_spending_lt_30"] / df["owner_occupied_households"]


def pct_ro_spending_lt_30(df):
    return 100 * df["ro_hh_spending_lt_30"] / df["renter_occupied_households"]


def pct_over_20_in_lf(df):
    return 100 * df["num_over_20_in_labor_force"] / df["population_over_20"]


def pct_16_to_19_in_lf(df):
    return 100 * df["num_16_to_19_in_labor_force"] / df["population_16_to_19"]


def pct_owner_occupied(df):
    return 100 * df["owner_occupied_households"] / df["total_households"]


def pct_renter_occupied(df):
    return 100 * df["renter_occupied_households"] / df["total_households"]


def pct_children_below_pov(df):
    return 100 * df["num_children_below_pov"] / (
        df["num_children_below_pov"] + df["num_children_above_pov"]
    )



def transform(logger):
    logger.info("Aggregating tract-level ACS data to 2026 Council Districts")




    FIELD_DEFAULTS = {
        "year": 2024, 
        "survey_id": pd.NA, 
        "survey_question_id": pd.NA, 
        "survey_question_option_id": pd.NA
    }

    aggregations = json.loads(
        (WORKING_DIR / "conf" / "aggregations.json").read_text()
    )

    instructions = json.loads(
        (WORKING_DIR / "conf" / "liquefy_instructions.json").read_text()
    )

    districts = pull_tracts_to_nvi_crosswalk(2020, 2026)

    city_wide = pd.read_parquet(
        WORKING_DIR / "output" / f"nvi_citywide_{YEAR}.parquet.gzip"
    ).reset_index()

    tract_level = pd.read_parquet(
        WORKING_DIR / "output" / f"nvi_tracts_{YEAR}.parquet.gzip"
    ).reset_index()
    
    
    # TODO Read this dynamically off of the locations file

    # Tract-level
    wide_format = (
        districts.rename(
            columns={
                "tract_geoid": "geoid",
                "district_number": "district",
            }
        )
        .astype({"geoid": "str"})
        .merge(tract_level, on="geoid")
        .groupby("district")
        .agg(aggregations["aggregations"])
        .assign(
            pct_hs_diploma_ged_plus=pct_hs_diploma_ged_plus,
            pct_post_secondary=pct_post_secondary,
            pct_oo_spending_lt_30=pct_oo_spending_lt_30,
            pct_ro_spending_lt_30=pct_ro_spending_lt_30,
            pct_over_20_in_lf=pct_over_20_in_lf,
            pct_16_to_19_in_lf=pct_16_to_19_in_lf,
            pct_owner_occupied=pct_owner_occupied,
            pct_renter_occupied=pct_renter_occupied,
            pct_children_below_pov=pct_children_below_pov,
            hierfindal=lambda df: hierfindal(roll_up_income_categories(df)),
            location_id=lambda df: df.apply(lambda row: pin_location('district', row), axis=1)
        )
    )

    assert len(wide_format) == 7

    result = liquefy(wide_format, instructions, defaults=FIELD_DEFAULTS)

    result.to_csv(
        WORKING_DIR / "output" / f"nvi_districts_{YEAR}.csv", index=False
    )

    # NVI Zones

    wide_format = (
        districts.rename(
            columns={
                "tract_geoid": "geoid",
                "district_number": "district",
                "zone_name": "zone",
            }
        )
        .astype({"geoid": "str"})
        .merge(tract_level, on="geoid")
        .groupby("zone")
        .agg(aggregations["aggregations"])
        .assign(
            pct_hs_diploma_ged_plus=pct_hs_diploma_ged_plus,
            pct_post_secondary=pct_post_secondary,
            pct_oo_spending_lt_30=pct_oo_spending_lt_30,
            pct_ro_spending_lt_30=pct_ro_spending_lt_30,
            pct_over_20_in_lf=pct_over_20_in_lf,
            pct_16_to_19_in_lf=pct_16_to_19_in_lf,
            pct_owner_occupied=pct_owner_occupied,
            pct_renter_occupied=pct_renter_occupied,
            pct_children_below_pov=pct_children_below_pov,
            hierfindal=lambda df: hierfindal(roll_up_income_categories(df)),
            location_id=lambda df: df.apply(lambda row: pin_location('zone', row), axis=1)
        )
    )

    assert len(wide_format) == 22

    result = liquefy(wide_format, instructions, defaults=FIELD_DEFAULTS)

    result.to_csv(
        WORKING_DIR / "output" / f"nvi_zones_{YEAR}.csv", index=False
    )

    # City-wide

    wide_format = (
        city_wide.assign(district=1)
        .groupby("district")
        .agg(aggregations["aggregations"])
        .assign(  # Assign the district-level aggs with the functions above
            pct_hs_diploma_ged_plus=pct_hs_diploma_ged_plus,
            pct_post_secondary=pct_post_secondary,
            pct_oo_spending_lt_30=pct_oo_spending_lt_30,
            pct_ro_spending_lt_30=pct_ro_spending_lt_30,
            pct_over_20_in_lf=pct_over_20_in_lf,
            pct_16_to_19_in_lf=pct_16_to_19_in_lf,
            pct_owner_occupied=pct_owner_occupied,
            pct_renter_occupied=pct_renter_occupied,
            pct_children_below_pov=pct_children_below_pov,
            hierfindal=lambda df: hierfindal(roll_up_income_categories(df)),
            location_id=1
        )
    )

    assert len(wide_format) == 1
    result = liquefy(wide_format, instructions, defaults=FIELD_DEFAULTS)

    result.to_csv(
        WORKING_DIR / "output" / f"nvi_citywide_{YEAR}.csv", index=False
    )
