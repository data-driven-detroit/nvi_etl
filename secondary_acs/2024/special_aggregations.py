import pandas as pd


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