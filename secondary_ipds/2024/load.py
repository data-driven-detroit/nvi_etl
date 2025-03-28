import pandas as pd
from nvi_etl import working_dir, db_engine
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import CONTEXT_VALUES_TABLE


WORKING_DIR = working_dir(__file__)


def load_from_queries(logger):
    logger.warning("Loading other ipds data into context values table.")

    file = pd.read_csv(WORKING_DIR / "output" / "ipds_tall_from_queries.csv")

    validated = NVIValueTable.validate(file)

    validated.to_sql(CONTEXT_VALUES_TABLE, db_engine, schema="nvi", index=False, if_exists="append")
