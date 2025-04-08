from nvi_etl import setup_logging
from extract import extract
from transform import transform
from load import load_acs


logger = setup_logging()

extract(logger)
transform(logger)
load_acs(logger)
