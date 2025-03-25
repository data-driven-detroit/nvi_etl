from nvi_etl import working_dir, db_engine
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import CONTEXT_VALUES_TABLE
import pandas as pd


WORKING_DIR = working_dir(__file__)


def load_mischooldata(logger):
    logger.warning("Loading mischooldata datasets!")

    file = pd.read_csv(WORKING_DIR / "output" / "g3_ela_2023_melted.csv")

    NVIValueTable.validate(file)

    file.to_sql(CONTEXT_VALUES_TABLE, db_engine, schema="nvi")
