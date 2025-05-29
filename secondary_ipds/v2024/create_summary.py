from pathlib import Path
import pandas as pd
from sqlalchemy import text
from nvi_etl import make_engine_for


WORKING_DIR = Path(__file__).resolve().parent


nvi_engine = make_engine_for("nvi_test")

location_q = text("""
SELECT id, name
FROM location;
""")

with nvi_engine.connect() as db:
    location_rows = db.execute(location_q)
    location_map = {r["id"]: r["name"] for r in location_rows.mappings().all()}


indicator_q = text("""
SELECT id, name
FROM indicator;
""")

with nvi_engine.connect() as db:
    indicator_rows = db.execute(indicator_q)
    indicator_map = {r["id"]: r["name"] for r in indicator_rows.mappings().all()}

foreclosures = pd.read_csv(WORKING_DIR / "output" / "foreclosures_tall.csv")
the_rest = pd.read_csv(WORKING_DIR / "output" / "ipds_tall_from_queries.csv")
df = pd.concat([foreclosures, the_rest])

(
    df.query("location_id in [2,3,4,5,6,7,8]")
    .assign(
        location=lambda df: df["location_id"].map(location_map),
        indicator=lambda df: df["indicator_id"].map(indicator_map)
    )[["location", "indicator", "count", "universe", "percentage", "index"]]
    .to_excel("nvi_ipds_primary_indicators_districts_20250515.xlsx", index=False)
)
