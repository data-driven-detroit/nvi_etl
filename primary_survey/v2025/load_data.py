from nvi_etl import working_dir, db_engine
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import SURVEY_VALUES_TABLE
import pandas as pd


WORKING_DIR = working_dir(__file__)


def load_data(logger):
    logger.warning("Loading NVI Survey data!")
    
    # Open file
    file = pd.read_csv(WORKING_DIR / "output" / "nvi_survey_2024.csv")
    
    NVIValueTable.validate(file) 
    
    file.to_sql(
        SURVEY_VALUES_TABLE, 
        db_engine, 
        schema="nvi", 
        if_exists="append", 
        index=False
    )

    logger.info("Data loaded successfully!")
