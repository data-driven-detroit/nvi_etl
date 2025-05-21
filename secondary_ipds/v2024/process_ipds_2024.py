from nvi_etl import setup_logging

from setup import (
    create_itermediate_table, 
    load_in_population_reference, 
    load_in_location_types
)
from extract import extract_foreclosures, extract_from_queries
from transform import transform_primary, transform_context
from load import load_from_queries, load_foreclosures


def main():
    logger = setup_logging()

    # SETUP Reference tables
    # Loads ACS b01003 2023 in for population reference
    load_in_population_reference(logger)
    # Load in land-use map table created by Noah
    load_in_location_types(logger)
    # Vacancy requires an intermediate table to be created
    create_itermediate_table(logger)

    extract_foreclosures(logger)
    extract_from_queries(logger)

    transform_primary(logger)
    transform_context(logger)

    # load_from_queries(logger)



if __name__ == "__main__":
    main()