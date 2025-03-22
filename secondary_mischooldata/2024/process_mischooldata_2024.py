from nvi_etl import setup_logging

from extract import extract_mischooldata
from transform import transform_mischooldata
from load import load_mischooldata


logger = setup_logging()


extract_mischooldata(logger)
transform_mischooldata(logger)
load_mischooldata(logger)

