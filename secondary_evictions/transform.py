from pathlib import Path
import pandas as pd

from nvi_etl.geo_reference import pin_location
from nvi_etl.schema import CONTEXT_VALUE_COLUMNS


WORKING_DIR = Path(__file__).resolve().parent


def transform(logger):
    logger.info("Transforming evictions.")
    frame = pd.read_csv(WORKING_DIR / "input" / "evictions_anon_point_level.csv")

    frame["year"] = pd.to_datetime(frame["filed_date"]).dt.year
    context_indicators = pd.read_csv(WORKING_DIR / "conf" / "context_indicator_ids.csv")

    aggregated = pd.concat(
        [
            (
                frame.groupby(["zone_id", "year"])
                .size()
                .rename("count")
                .reset_index()
                .rename(columns={"zone_id": "geography"})
                .assign(geo_type="zone")
            ),
            (
                frame.groupby(["district_number", "year"])
                .size()
                .rename("count")
                .reset_index()
                .rename(columns={"district_number": "geography"})
                .astype({"geography": "str"})
                .assign(geo_type="district")
            ),
            (
                frame.groupby(["year"])
                .size()
                .rename("count")
                .reset_index()
                .assign(geo_type="citywide", geography="Detroit")
            ),
        ]
    )


    (
        aggregated
        .astype({"year": pd.Int64Dtype()})
        .assign(
            location_id=lambda df: df.apply(pin_location, axis=1)
        )
        .merge(context_indicators, on=["year"]) # Only one indicator
        .assign(
            universe=pd.NA,
            percentage=pd.NA,
            rate=pd.NA,
            rate_per=pd.NA,
            dollars=pd.NA,
            index=pd.NA,
        )
        [CONTEXT_VALUE_COLUMNS]
        .to_csv(WORKING_DIR / "output" / "evictions.csv", index=False)
    )


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform(logger)