from nvi_etl import setup_logging

from setup import create_itermediate_table
from extract import extract_from_queries
from transform import transform_ipds
from load import load_ipds


logger = setup_logging()

# Vacancy requires an intermediate table to be created
create_itermediate_table(logger)

extract_from_queries(logger)
# transform_ipds(logger)
# load_ipds(logger)
