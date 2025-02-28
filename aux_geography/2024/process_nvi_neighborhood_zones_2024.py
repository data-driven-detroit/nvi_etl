from nvi_etl import setup_logging

from extract import extract_neighborhood_zones
from transform import transform_neighborhood_zones
from validate import validate_neighborhood_zones
from load import load_neighborhood_zones


logger = setup_logging()

# Neighborhood zone workflow
extract_neighborhood_zones(logger)
transform_neighborhood_zones(logger)
validate_neighborhood_zones(logger)
load_neighborhood_zones(logger)

