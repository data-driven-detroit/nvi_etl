from nvi_etl import setup_logging

from extract import create_intermediate_table, extract_ipds
from transform import transform_ipds
from load import load_ipds


logger = setup_logging()

create_intermediate_table(logger)
extract_ipds(logger)
transform_ipds(logger)
load_ipds(logger)

