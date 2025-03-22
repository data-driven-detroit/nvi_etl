from nvi_etl import setup_logging

from extract import extract_ipds
from transform import transform_ipds
from load import load_ipds


logger = setup_logging()


extract_ipds(logger)
transform_ipds(logger)
load_ipds(logger)

