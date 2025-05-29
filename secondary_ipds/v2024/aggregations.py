import pandas as pd


def build_pct_calculator(indicator, cent_scale=False):
    def inner(df):
        numerator = df[f"count_{indicator}"] * (100 if cent_scale else 1)
        denominator = df[f"universe_{indicator}"]

        try:
            return numerator / denominator

        except ZeroDivisionError:
            return pd.NA

    return  inner


def compile_indicators(indicators: pd.DataFrame, logger):
    aggregations = {}
    for _, indicator in indicators.iterrows():
        if indicator["indicator_type"] == "count":
            continue # counts don't need aggregations
        elif indicator["indicator_type"] in {"percentage", "rate"}:
            function = build_pct_calculator(indicator["indicator"])
        else:
            logger.error(f"'{indicator["indicator_type"]}' is not a valid indicator type for IPDS. '{indicator["indicator"]}': SKIPPING")
            continue

        aggregations[
            f"{indicator["indicator_type"]}_{indicator["indicator"]}"
        ] = function

    return aggregations