import json
from nvi_etl import working_dir
import pandas as pd


WORKING_DIR = working_dir(__file__)
location_map = json.loads((WORKING_DIR / "conf" / "location_map.json").read_text())
primary_indicators = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv")

def pin_location_id(row):
    return location_map[row["geo_type"]][row["geography"]]


def transform_mischooldata(logger):
    logger.info("Transforming mischooldata datasets.")

    wide_file = (
        pd.read_csv(WORKING_DIR / "input" / "g3_ela_2023_extract.csv")
        .assign(
            location_id=lambda df: df.apply(pin_location_id, axis=1)
        )
    )

    wide_file.to_csv(WORKING_DIR / "output" / "g3_ela_2023_wide.csv", index=False)

    melted = (
        pd.wide_to_long(
            wide_file,
            stubnames=["count", "universe", "percentage", "rate", "per", "dollars", "index"],
            i=["location_id", "year"],
            j="indicator",
            sep="_",
            suffix=".*",
        )
        .reset_index()
        .rename(columns={"per": "rate_per"})
        .merge(primary_indicators, on=["indicator", "year"], how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .assign(value_type_id=1)
    )

    melted.to_csv(WORKING_DIR / "output" / "g3_ela_2023_tall.csv", index=False)


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform_mischooldata(logger)