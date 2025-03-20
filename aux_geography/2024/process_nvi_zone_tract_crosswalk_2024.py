from nvi_etl import setup_logging

from extract import extract_2020_tracts_to_2026_nvi_cw
from transform import transform_2020_tracts_to_2026_nvi_cw
from load import load_2020_tracts_to_2026_nvi_cw

"""
This file crosswalks 2026 council districts and nvi zones to 2020 tracts.
"""

logger = setup_logging()

# TODO: Prepare a file for old council districts?
# TODO: Prepare a similar file / process for 2010 tracts (& corresponding old council districts)?

extract_2020_tracts_to_2026_nvi_cw(logger)
transform_2020_tracts_to_2026_nvi_cw(logger)
load_2020_tracts_to_2026_nvi_cw(logger)
