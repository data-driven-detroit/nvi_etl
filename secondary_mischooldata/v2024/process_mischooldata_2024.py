from nvi_etl import setup_logging, working_dir, extract_from_sql_file, db_engine
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import CONTEXT_VALUES_TABLE, SURVEY_VALUES_TABLE
import pandas as pd
import json

#from extract import extract_mischooldata
#from transform import transform_mischooldata
#from load import load_mischooldata

# Use this path to find sql files
WORKING_DIR = working_dir(__file__)
location_map = json.loads((WORKING_DIR / "conf" / "location_map.json").read_text())
primary_indicators = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv")

# Extract
def extract_mischooldata(logger):
    logger.info("Extracting mischooldata datasets!")
    
    query_files = [
        "third_grade_ela_citywide.sql",
        "third_grade_ela_council_districts.sql",
        "thrid_grade_ela_neighborhood_zones.sql",
    ]

    g3_ela_tables = []
    for file in query_files:
        g3_ela_tables.append(extract_from_sql_file(WORKING_DIR / "sql" / file))
    
    combined = pd.concat(g3_ela_tables)
    combined.to_csv(WORKING_DIR / "input" / "g3_ela_2023_extract.csv", index=False)


# Transform
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

# Load 

def load_mischooldata(logger):
    logger.info("Loading mischooldata datasets!")

    file = pd.read_csv(WORKING_DIR / "output" / "g3_ela_2023_melted.csv")

    NVIValueTable.validate(file)

    file.to_sql(
        SURVEY_VALUES_TABLE, 
        db_engine, 
        schema="nvi", 
        if_exists="append", 
        index=False
    )

    
def main():
    logger = setup_logging()


    extract_mischooldata(logger)
    transform_mischooldata(logger)
    # load_mischooldata(logger)

if __name__ == "__main__":
    main()