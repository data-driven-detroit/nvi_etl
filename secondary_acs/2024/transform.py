from pathlib import Path
import pandas as pd
from sqlalchemy import text

from nvi_etl import db_engine


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
    ]


def pct_post_secondary(df):
    return (
        df["population_ge_25_with_post_sec_degree"]
        / df["total_population_ge_25"]
    )


def pct_oo_spending_lt_30(df):
    return df["oo_hh_spending_lt_30"] / df["owner_occupied_households"]


def pct_ro_spending_lt_30(df):
    return df["ro_hh_spending_lt_30"] / df["renter_occupied_households"]


def pct_over_20_in_lf(df):
    return df["num_over_20_in_labor_force"] / df["population_over_20"]


def pct_16_to_19_in_lf(df):
    return df["num_16_to_19_in_labor_force"] / df["population_16_to_19"]


def pct_owner_occupied(df):
    return df["owner_occupied_households"] / df["total_households"]


def pct_renter_occupied(df):
    return df["renter_occupied_households"] / df["total_households"]


def pct_children_below_pov(df):
    return df["num_children_below_pov"] / (
        df["num_children_below_pov"] + df["num_children_above_pov"]
    )


def pull_tracts_to_new_council_districts():
    ref_q = text("""
    SELECT *
    FROM blah.blah;
    """)

    with 


def transform(logger):

    # TODO: This file is going to actually be from the database.
    districts = pd.read_parquet(WORKING_DIR / f"tracts_to_council_districts_{YEAR}.csv")[
        ["geoid", "district"]
    ]

    city_wide = pd.read_parquet(
        f"../input/nvi_citywide_{YEAR}.parquet.gzip"
    ).reset_index()
    tract_level = pd.read_parquet(
        f"../input/nvi_tracts_{YEAR}.parquet.gzip"
    ).reset_index()

    # Tract-level
    result = (
        districts.astype({"geoid": "str"})
        .merge(tract_level, on="geoid")
        .groupby("district")
        .agg(
            {
                "num_people_w_det_poverty_status": "sum",
                "num_people_below_200_fpl_acs": "sum",
                "num_people_above_200_fpl_acs": "sum",
                "total_households": "sum",
                "owner_occupied_households": "sum",
                "oo_hh_spending_lt_30": "sum",
                "renter_occupied_households": "sum",
                "ro_hh_spending_lt_30": "sum",
                "total_population_ge_25": "sum",
                "population_ge_25_with_post_hs_diploma_ged": "sum",
                "population_ge_25_with_post_sec_degree": "sum",
                "population_over_20": "sum",
                "num_over_20_in_labor_force": "sum",
                "population_16_to_19": "sum",
                "num_16_to_19_in_labor_force": "sum",
                "total_population": "sum",
                "income_lt_10000": "sum",
                "income_10000_to_14999": "sum",
                "income_20000_to_24999": "sum",
                "income_25000_to_29999": "sum",
                "income_30000_to_34999": "sum",
                "income_35000_to_39999": "sum",
                "income_40000_to_44999": "sum",
                "income_45000_to_49999": "sum",
                "income_50000_to_59999": "sum",
                "income_60000_to_74999": "sum",
                "income_75000_to_99999": "sum",
                "income_100000_to_124999": "sum",
                "income_125000_to_149999": "sum",
                "income_150000_to_199999": "sum",
                "income_ge_200000": "sum",
                "num_white_alone": "sum",
                "num_black_or_african_american_alone": "sum",
                "num_american_indian_and_alaska_native_alone": "sum",
                "num_asian_alone": "sum",
                "num_native_hawaiian_and_other_pacific_islander_alone": "sum",
                "num_some_other_race_alone": "sum",
                "num_two_or_more_races": "sum",
                "num_hispanic_or_latino": "sum",
                "num_children_below_pov": "sum",
                "num_children_above_pov": "sum",
                "num_households_with_children": "sum",
            }
        )
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
        )
    )

    assert len(result) == 7
    result.to_excel(f"./output/nvi_districts_{YEAR}.xlsx")

    # City-wide

    result = (
        city_wide.assign(district=1)
        .groupby("district")
        .agg(
            {
                "num_people_w_det_poverty_status": "sum",
                "num_people_below_200_fpl_acs": "sum",
                "num_people_above_200_fpl_acs": "sum",
                "total_households": "sum",
                "owner_occupied_households": "sum",
                "oo_hh_spending_lt_30": "sum",
                "renter_occupied_households": "sum",
                "ro_hh_spending_lt_30": "sum",
                "total_population_ge_25": "sum",
                "population_ge_25_with_post_hs_diploma_ged": "sum",
                "population_ge_25_with_post_sec_degree": "sum",
                "population_over_20": "sum",
                "num_over_20_in_labor_force": "sum",
                "population_16_to_19": "sum",
                "num_16_to_19_in_labor_force": "sum",
                "total_population": "sum",
                "income_lt_10000": "sum",
                "income_10000_to_14999": "sum",
                "income_20000_to_24999": "sum",
                "income_25000_to_29999": "sum",
                "income_30000_to_34999": "sum",
                "income_35000_to_39999": "sum",
                "income_40000_to_44999": "sum",
                "income_45000_to_49999": "sum",
                "income_50000_to_59999": "sum",
                "income_60000_to_74999": "sum",
                "income_75000_to_99999": "sum",
                "income_100000_to_124999": "sum",
                "income_125000_to_149999": "sum",
                "income_150000_to_199999": "sum",
                "income_ge_200000": "sum",
                "num_white_alone": "sum",
                "num_black_or_african_american_alone": "sum",
                "num_american_indian_and_alaska_native_alone": "sum",
                "num_asian_alone": "sum",
                "num_native_hawaiian_and_other_pacific_islander_alone": "sum",
                "num_some_other_race_alone": "sum",
                "num_two_or_more_races": "sum",
                "num_hispanic_or_latino": "sum",
                "num_children_below_pov": "sum",
                "num_children_above_pov": "sum",
                "num_households_with_children": "sum",
            }
        )
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
        )
    )

    assert len(result) == 1
    result.to_excel(f"./output/nvi_citywide_{YEAR}.xlsx")
