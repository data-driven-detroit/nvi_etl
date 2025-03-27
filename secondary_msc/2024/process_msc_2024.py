from nvi_etl import setup_logging

from extract_msc import extract_births, extract_from_queries
from transform_msc import transform_births, transform_from_queries
from load_msc import load_births, load_from_queries


logger = setup_logging()


logger.info("Starting births")
extract_births(logger)
transform_births(logger)
load_births(logger)


logger.info("Starting pulls from queries")
extract_from_queries(logger)
transform_from_queries(logger)
load_from_queries(logger)
