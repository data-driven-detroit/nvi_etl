from nvi_etl import setup_logging

from extract import extract_city_boundary
from transform import transform_city_boundary
from load import load_city_boundary


logger = setup_logging()

extract_city_boundary(logger)
transform_city_boundary(logger)
load_city_boundary(logger)
