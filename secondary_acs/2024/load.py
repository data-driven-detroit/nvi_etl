from pathlib import Path
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from nvi_etl import db_engine
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import CONTEXT_VALUES_TABLE


SCHEMA = "nvi"
WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023


def load_acs(logger):
    logger.info("Checking for conflicts and pushing tables to database.")

    citywide = pd.read_csv(WORKING_DIR / "output" / f"nvi_citywide_{YEAR}.csv")
    districts = pd.read_csv(
        WORKING_DIR / "output" / f"nvi_districts_{YEAR}.csv"
    )
    zones = pd.read_csv(WORKING_DIR / "output" / f"nvi_zones_{YEAR}.csv")

    df = pd.concat([citywide, districts, zones])

    validated = NVIValueTable.validate(df)

    prev_q = text(
        f"""
        SELECT count(*) AS matches_found
        FROM {SCHEMA}.{CONTEXT_VALUES_TABLE}
        WHERE (indicator_id, year, location_id) IN :check_against
    """
    )

    logger.info("Checking for repeated row inserts.")

    check_against = list(
        validated[["indicator_id", "year", "location_id"]].itertuples(
            index=False
        )
    )

    with db_engine.connect() as db:
        try:
            result = db.execute(prev_q, {"check_against": check_against})

            matches_found = result.fetchone()

            if matches_found and (matches_found != 0):
                logger.error(
                    "Some rows from current file have conflicting keys on "
                    "'district_code', 'building_code', and 'start_date'. "
                    "Remove these rows on the table before continuing."
                )

                raise AssertionError(
                    "Resolve database-new upload conflicts and try again."
                )

        except ProgrammingError:
            logger.info(
                "First time table is being pushed, so no row-level conflicts."
            )

    with db_engine.connect() as db:
        validated.to_sql(
            CONTEXT_VALUES_TABLE, db, schema=SCHEMA, index=False, if_exists="append"
        )
