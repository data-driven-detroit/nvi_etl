from nvi_etl import setup_logging
from extract import (
    extract_2020_tracts_to_2014_council_districts,
    extract_2010_tracts_to_2014_council_districts,
)
from transform import (
    transform_2020_tracts_to_2014_council_districts,
    transform_2010_tracts_to_2014_council_districts,
)
from validate import (
    validate_2010_tract_2014_cd_crosswalk,
    validate_2020_tract_2014_cd_crosswalk,
)
from load import (
    load_2020_tracts_to_2014_council_districts,
    load_2010_tracts_to_2014_council_districts,
)

"""
This file crosswalks 2014 council districts to 2010 and 2020 tracts.
"""

logger = setup_logging()

extract_2010_tracts_to_2014_council_districts(logger)
transform_2010_tracts_to_2014_council_districts(logger)
validate_2010_tract_2014_cd_crosswalk(logger)
load_2010_tracts_to_2014_council_districts(logger)

extract_2020_tracts_to_2014_council_districts(logger)
transform_2020_tracts_to_2014_council_districts(logger)
validate_2020_tract_2014_cd_crosswalk(logger)
load_2020_tracts_to_2014_council_districts(logger)

