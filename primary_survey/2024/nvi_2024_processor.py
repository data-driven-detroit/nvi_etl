from nvi_etl import setup_logging
from extract_data import extract_data
from validate_data import validate_data
from transform_data import transform_data
from load_data import load_data


logger = setup_logging()


extract_data(logger)
validate_data(logger)
transform_data(logger)
load_data(logger)
