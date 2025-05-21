import configparser
import pandas as pd

from nvi_etl import working_dir
from nvi_etl.reshape import elongate
from nvi_etl.geo_reference import pin_location

from aggregations import compile_indicators


WORKING_DIR = working_dir(__file__)


conf = configparser.ConfigParser()
conf.read(WORKING_DIR / "conf" / ".conf")


def transform_foreclosures(primary_indicators, logger):
    logger.info("Transforming foreclosures into 'tall' format.")

    wide_file = (
        pd.read_csv(WORKING_DIR / "input" / "foreclosures_wide.csv")
        .assign(
            location_id=lambda df: df.apply(pin_location, axis=1)
        )
    )

    tall_file = (
        elongate(wide_file)
        .merge(primary_indicators, on=["indicator", "year"], how="left")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
    )

    tall_file.to_csv(WORKING_DIR / "output" / "foreclosures_tall.csv", index=False)


def read_location_pinned_file():
    return (
        pd.read_csv(WORKING_DIR / "input" / "ipds_wide_from_queries.csv")
        .assign(
            location_id=lambda df: df.apply(pin_location, axis=1)
        )
    )


def transform_from_queries(primary_indicators, logger):
    logger.info("Transforming IPDS query output into tall format.")

    wide_file = read_location_pinned_file() 

    tall_file = (
        elongate(wide_file)
        .merge(primary_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
    )

    tall_file.to_csv(WORKING_DIR / "output" / "ipds_primary_tall_from_queries.csv", index=False)


def transform_primary(logger):
    primary_indicators = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv")

    transform_foreclosures(primary_indicators, logger)
    transform_from_queries(primary_indicators, logger)


def transform_context(logger):
    logger.info("Transforming IPDS context indicators.")

    context_indicators = pd.read_csv(WORKING_DIR / "conf" / "context_indicator_ids.csv")
    indicators = compile_indicators(context_indicators, logger)

    wide_file = read_location_pinned_file().assign(**indicators)

    tall_file = (
        elongate(wide_file)
        .merge(context_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
    )

    tall_file.to_csv(WORKING_DIR / "output" / "ipds_context_tall_from_queries.csv", index=False)


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform_primary(logger)
    transform_context(logger)