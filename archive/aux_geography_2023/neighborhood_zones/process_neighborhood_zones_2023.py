from nvi_etl import setup_logging

from transform import transform_neighborhood_zones
from load import load_neighborhood_zones


logger = setup_logging()

transform_neighborhood_zones(logger)
load_neighborhood_zones(logger)
