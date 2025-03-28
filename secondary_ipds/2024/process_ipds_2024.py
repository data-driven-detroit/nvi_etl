from nvi_etl import setup_logging

from setup import create_itermediate_table, load_in_population_reference
from extract import extract_from_queries
from transform import transform_from_queries
from load import load_from_queries


logger = setup_logging()

# SETUP Reference tables
# Population reference table has to be created
load_in_population_reference(logger)

# Vacancy requires an intermediate table to be created
create_itermediate_table(logger)

extract_from_queries(logger)
transform_from_queries(logger)
load_from_queries(logger)
