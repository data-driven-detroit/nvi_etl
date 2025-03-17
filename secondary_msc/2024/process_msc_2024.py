from nvi_etl import setup_logging

from extract_msc import extract_births
from transform_msc import transform_births
from load_msc import load_births


logger = setup_logging()


extract_births(logger)
transform_births(logger)
load_births(logger)

