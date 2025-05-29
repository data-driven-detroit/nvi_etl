import pandas as pd

from nvi_etl.utilities import estimate_median_from_distribution


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


def estimate_median_housing_value(row):
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


def estimate_median_gross_rent(row):
    distribution = [
        ((0, 100), row["gross_rent_1"]),
        ((100, 149), row["gross_rent_2"]),
        ((150, 199), row["gross_rent_3"]),
        ((200, 249), row["gross_rent_4"]),
        ((250, 299), row["gross_rent_5"]),
        ((300, 349), row["gross_rent_6"]),
        ((350, 399), row["gross_rent_7"]),
        ((400, 449), row["gross_rent_8"]),
        ((450, 499), row["gross_rent_9"]),
        ((500, 549), row["gross_rent_10"]),
        ((550, 599), row["gross_rent_11"]),
        ((600, 649), row["gross_rent_12"]),
        ((650, 699), row["gross_rent_13"]),
        ((700, 749), row["gross_rent_14"]),
        ((750, 799), row["gross_rent_15"]),
        ((800, 899), row["gross_rent_16"]),
        ((900, 999), row["gross_rent_17"]),
        ((1000, 1249), row["gross_rent_18"]),
        ((1250, 1499), row["gross_rent_19"]),
        ((1500, 1999), row["gross_rent_20"]),
        ((2000, 2499), row["gross_rent_21"]),
        ((2500, 2999), row["gross_rent_22"]),
        ((3000, 3499), row["gross_rent_23"]),
        ((3500, float("inf")), row["gross_rent_24"]),
    ]

    return estimate_median_from_distribution(distribution)



def build_pct_calculator(indicator, cent_scale=False):
    def inner(df):
        numerator = df[f"count_{indicator}"] * (100 if cent_scale else 1)
        denominator = df[f"universe_{indicator}"]

        try:
            return numerator / denominator

        except ZeroDivisionError:
            return pd.NA

    return  inner


def build_hierfindal_calc(): # Allow indicator string to be passed
    def inner(df):
        return hierfindal(roll_up_income_categories(df))

    return inner


def build_median_calc(strategy):
    def inner(df):
        return df.apply(strategy, axis=1)
    
    return inner


def compile_indicators(indicators: pd.DataFrame, logger):
    """
    This is kind of messy, and might require a strategy pattern down the road.
    """
    aggregations = {}
    for _, indicator in indicators.iterrows():
        if indicator["indicator_type"] == "count":
            continue # counts don't need aggregations
        elif indicator["indicator_type"] in {"percentage", "rate"}:
            # Hot take -- percentages are rates (per cent (100))
            function = build_pct_calculator(indicator["indicator"])
        elif indicator["indicator_type"] == "index":
            if indicator["indicator"] == "hierfindal":
                function = build_hierfindal_calc()
            else:
                logger.error(f"'{indicator["indicator"]}' doesn't have a assigned aggregator: SKIPPING")
        elif indicator["indicator_type"] == "dollars":
            if indicator["indicator"] == "median_housing_value":
                function = build_median_calc(estimate_median_housing_value)
            elif indicator["indicator"] == "median_gross_rent":
                function = build_median_calc(estimate_median_gross_rent)
        else:
            logger.error(f"'{indicator["indicator_type"]}' is not a valid indicator type. '{indicator["indicator"]}': SKIPPING")
            continue

        aggregations[
            f"{indicator["indicator_type"]}_{indicator["indicator"]}"
        ] = function

    return aggregations