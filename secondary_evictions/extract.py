from pathlib import Path
import pandas as pd
from sqlalchemy import text

from nvi_etl import make_engine_for


WORKING_DIR = Path(__file__).resolve().parent


def extract(logger):
    evictions_db = make_engine_for("evictions")
    ipds_db = make_engine_for("ipds")

    evictions_query = text((WORKING_DIR / "sql" / "evictions.sql").read_text())
    parcel_query = text((WORKING_DIR / "sql" / "parcel_labels.sql").read_text())

    evictions = pd.read_sql(evictions_query, evictions_db)
    parcels = pd.read_sql(parcel_query, ipds_db)

    point_level = evictions.merge(parcels, on="d3_id", how="inner")

    point_level.to_csv(WORKING_DIR / "input" / "evictions_anon_point_level.csv", index=False)


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    extract(logger)