from nvi_etl import setup_logging
from extract import extract_council_districts, extract_neighborhood_zones
from transform import transform_council_districts, transform_neighborhood_zones
from validate import validate_council_districts, validate_neighborhood_zones
from load import load_council_districts, load_neighborhood_zones

"""
These are required to run before the rest of the scripts in the file, because
the aggregations are dependent on having the appropriate zones.
"""

logger = setup_logging()

# Council Districts workflow
extract_council_districts(logger)
transform_council_districts(logger)
validate_council_districts(logger)
load_council_districts(logger)


