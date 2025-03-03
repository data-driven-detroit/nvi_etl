from nvi_etl import setup_logging
from extract import extract
from transform import transform
from validate import validate_acs_to_nvi_schema
from load import load_acs
from archive import archive_acs


logger = setup_logging()

extract(logger)
transform(logger)
validate_acs_to_nvi_schema(logger)
load_acs(logger)
archive_acs(logger)

