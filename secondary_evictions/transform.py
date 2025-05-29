from pathlib import Path
import pandas as pd


WORKING_DIR = Path(__file__).resolve().parent



def transform(logger):
    frame = pd.read_csv(WORKING_DIR / "input" / "evictions_anon_point_level.csv")

    print(frame.head())


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform(logger)