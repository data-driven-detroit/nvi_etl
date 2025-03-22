from nvi_etl import working_dir, extract_from_sql_file
import pandas as pd


# Use this path to find sql files
WORKING_DIR = working_dir(__file__)


def extract_mischooldata(logger):
    logger.warning("Extracting mischooldata datasets!")
    
    query_files = [
        "third_grade_ela_citywide.sql",
        "third_grade_ela_council_districts.sql",
        "thrid_grade_ela_neighborhood_zones.sql",
    ]

    g3_ela_tables = []
    for file in query_files:
        g3_ela_tables.append(extract_from_sql_file(file))
    
    combined = pd.concat(g3_ela_tables)

    combined.to_csv(WORKING_DIR / "output" / "g3_ela_2023.csv")


