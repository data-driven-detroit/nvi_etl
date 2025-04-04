from nvi_etl import setup_logging

from setup import create_itermediate_table, load_in_population_reference
from extract import extract_foreclosures, extract_from_queries
from transform import transform_from_queries, transform_foreclosures
from load import load_from_queries, load_foreclosures


logger = setup_logging()

# SETUP Reference tables
# Population reference table has to be created
load_in_population_reference(logger)

# Vacancy requires an intermediate table to be created
create_itermediate_table(logger)

# ETL For queries
extract_from_queries(logger)
transform_from_queries(logger)
load_from_queries(logger)

# ETL For foreclosures
extract_foreclosures(logger)
transform_foreclosures(logger)
load_foreclosures(logger)