from pathlib import Path
import pandas as pd
from nvi_etl import db_engine
from validate import TractsToNVICrosswalk

WORKING_DIR = Path(__file__).resolve().parent

def load_2020_tracts_to_2026_nvi_cw(logger):
    logger.info("Loading transformed file to the nvi schema on the database.")

    file = pd.read_csv(
        WORKING_DIR / "output" / "tracts_2020_to_zones_2026_crosswalk.csv",
    )

    validated = TractsToNVICrosswalk.validate(file)

    validated.to_sql(
        "tracts_to_nvi_crosswalk", db_engine, schema="nvi", if_exists="append", index=False
    )
