from nvi_etl import setup_logging, working_dir, extract_from_sql_file, db_engine
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import CONTEXT_VALUES_TABLE, SURVEY_VALUES_TABLE
import pandas as pd
import json
from datetime import date

START_DATE = date(2024, 7, 1)
GEOMETRY_START_DATE = date(2026, 1, 1)
YEAR = 2025

# Use this path to find sql files
WORKING_DIR = working_dir(__file__)
location_map = json.loads((WORKING_DIR / "conf" / "location_map.json").read_text())
primary_indicators = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv")

# Extract
def extract_mischooldata(logger):
    logger.info("Extracting mischooldata datasets!")

    params = {
        "start_date": START_DATE,
        "geometry_start_date": GEOMETRY_START_DATE,
        "year": YEAR,
    }
    
    
    query_file =  "third_grade_ela_combined.sql"
    combined = pd.DataFrame(extract_from_sql_file(WORKING_DIR / "sql" / query_file,
                    params=params))
    combined.to_csv(WORKING_DIR / "input" / f"g3_ela_{YEAR}_extract.csv", index=False)


# Transform
def pin_location_id(row):
    return location_map[row["geo_type"]][row["geography"]]

def transform_mischooldata(logger):
    logger.info("Transforming mischooldata datasets.")

    wide_file = (
        pd.read_csv(WORKING_DIR / "input" / f"g3_ela_{YEAR}_extract.csv")
        .assign(
            location_id=lambda df: df.apply(pin_location_id, axis=1)
        )
    )

    wide_file.to_csv(WORKING_DIR / "output" / f"g3_ela_{YEAR}_wide.csv", index=False)

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

    melted.to_csv(WORKING_DIR / "output" / f"g3_ela_{YEAR}_tall.csv", index=False)

# Load 

def load_mischooldata(logger):
    logger.info("Loading mischooldata datasets!")

    file = pd.read_csv(WORKING_DIR / "output" / f"g3_ela_{YEAR}_melted.csv")

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