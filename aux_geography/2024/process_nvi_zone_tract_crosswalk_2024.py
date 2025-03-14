from nvi_etl import setup_logging
from extract import (
    extract_2020_tracts_to_2026_nvi_zones,
    extract_2010_tracts_to_2026_nvi_zones,
)
from validate import (
    validate_2010_tract_2026_zone_crosswalk,
    validate_2020_tract_2026_zone_crosswalk,
)
from load import (
    load_2020_tracts_to_2026_nvi_zones,
    load_2010_tracts_to_2026_nvi_zones,
)

"""
This file crosswalks 2026 council districts to 2010 and 2020 tracts.
"""

logger = setup_logging()

extract_2010_tracts_to_2026_nvi_zones(logger)
# transform_2010_tracts_to_2026_nvi_zones(logger)
validate_2010_tract_2026_zone_crosswalk(logger)
load_2010_tracts_to_2026_nvi_zones(logger)

extract_2020_tracts_to_2026_nvi_zones(logger)
# transform_2020_tracts_to_2026_nvi_zones(logger)
validate_2020_tract_2026_zone_crosswalk(logger)
load_2020_tracts_to_2026_nvi_zones(logger)

