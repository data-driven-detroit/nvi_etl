from nvi_etl import setup_logging
from extract import extract_council_districts
from transform import transform_council_districts
from load import load_council_districts


logger = setup_logging()

extract_council_districts(logger)
transform_council_districts(logger)
load_council_districts(logger)
