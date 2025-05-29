from pathlib import Path
import pandas as pd


WORKING_DIR = Path(__file__).resolve().parent



def transform(logger):
    frame = pd.read_csv(WORKING_DIR / "input" / "evictions_anon_point_level.csv")
    frame["year"] = pd.to_datetime(frame["filed_date"]).dt.year

    pd.concat(
        [
            frame.groupby(["zone_id", "year"], dropna=False).size().rename(columns={"zone_id": "geography"}),
            frame.groupby(["district_number", "year"], dropna=False).size().rename(columns={"district_number": "geography"}),
            frame.groupby(["year"], dropna=False).size(),
        ]


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform(logger)