from pathlib import Path
import pandas as pd

from nvi_etl.reshape import elongate
from nvi_etl.geo_reference import pull_tracts_to_nvi_crosswalk, pin_location
from aggregations import compile_indicators


WORKING_DIR = Path(__file__).resolve().parent


def diminish_geoid(geoid: str):
    """
    Until we institute a system-wide change to use the longer geoids, 
    we have to knock out a couple of 0s from the front of the geoid.

                               v we're removing these
    Returned by census: 14000 [00] US 26 163 511400
    What we want:       14000      US 26 163 511400
    """

    first, second = geoid.split("US")

    return first[:5] + "US" + second


def build_geography_groups():
    # On this one we want just these two files put together

    wide_file = pd.read_csv(WORKING_DIR / "input" / "nvi_2024_acs.csv")
    wide_file["geoid"] = wide_file["geoid"].apply(diminish_geoid)

    districts = (
        pull_tracts_to_nvi_crosswalk(2020, 2026)
        .rename(columns={"tract_geoid": "geoid"})
        .astype({"geoid": "str"})
    )

    sum_cols = {col: "sum" for col in wide_file.columns if col not in {"Geography Name", "Year", "Release", "geoid"}}

    geographies = []
    for geo_type, agg in [("zone", "zone_name"), ("district", "district_number")]:
        geographies.append(
            districts
            .merge(wide_file, on="geoid", how="left")
            .rename(columns={agg: "geography", "Year": "year"})
            .groupby(["geography", "year"])
            .agg(sum_cols)
            .assign(geo_type=geo_type)
            .reset_index()
        )

    # Add city-wide too
    geographies.append(
        wide_file.query("geoid == '06000US2616322000'")
        .rename(columns={"Year": "year"})
        .groupby("year")
        .agg(sum_cols)
        .assign(
            geography="Detroit",
            geo_type="citywide"
        )
        .reset_index()
    )

    return pd.concat(geographies)



def transform(logger):
    logger.info("Transforming NVI ACS part 2")

    geography_counts = build_geography_groups()

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
