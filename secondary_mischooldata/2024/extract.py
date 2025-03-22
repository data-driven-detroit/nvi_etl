from nvi_etl import db_engine, working_dir
import pandas as pd
from sqlalchemy import text


# Use this path to find sql files
WORKING_DIR = working_dir(__file__)


def extract_from_file(filename):
    q = text((WORKING_DIR / "sql" / filename).read_text())

    return pd.read_sql(q, db_engine)


def extract_mischooldata(logger):
    logger.warning("Extracting mischooldata datasets!")
    
    query_files = [
        "third_grade_ela_citywide.sql",
        "third_grade_ela_council_districts.sql",
        "thrid_grade_ela_neighborhood_zones.sql",
    ]

    g3_ela_tables = []
    for file in query_files:
        g3_ela_tables.append(extract_from_file(file))
    
    combined = pd.concat(g3_ela_tables)

    combined.to_csv(WORKING_DIR / "output" / "g3_ela_2023.csv")


