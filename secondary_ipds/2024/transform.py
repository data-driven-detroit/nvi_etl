from nvi_etl import working_dir, liquefy
from nvi_etl.geo_reference import pin_location
from nvi_etl.utilities import fix_parcel_id
import configparser
import pandas as pd
from shapely import wkb # FIXME Let's depend on the parcel file once it's complete


WORKING_DIR = working_dir(__file__)


conf = configparser.ConfigParser()
conf.read(WORKING_DIR / "conf" / ".conf")


def transform_foreclosures(logger):
    logger.info("Transforming foreclosures into 'tall' format.")

    wide_file = (
        pd.read_csv(WORKING_DIR / "input" / "foreclosures_wide.csv")
        .assign(
            location_id=lambda df: df.apply(pin_location, axis=1)
        )
    )

    tall_file = liquefy(wide_file).assign(year=2024) # Too much magic?
    tall_file.to_csv(WORKING_DIR / "output" / "foreclosures_tall.csv", index=False)


def transform_from_queries(logger):
    logger.info("Transforming IPDS query output into tall format.")

    wide_file = (
        pd.read_csv(WORKING_DIR / "input" / "ipds_wide_from_queries.csv")
        .assign(
            location_id=lambda df: df.apply(pin_location, axis=1)
        )
    )

    tall_file = liquefy(wide_file).assign(year=2024) # Too much magic?
    tall_file.to_csv(WORKING_DIR / "output" / "ipds_tall_from_queries.csv", index=False)
