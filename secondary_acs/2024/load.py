from pathlib import Path
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from nvi_etl import make_engine_for
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import CONTEXT_VALUES_TABLE, SURVEY_VALUES_TABLE


WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023


def load_acs(logger):
    db_engine = make_engine_for("data")

    logger.info("Checking for conflicts and pushing tables to database.")

    df = pd.read_csv(WORKING_DIR / "output" / "acs_primary_indicators_tall.csv")

    validated = NVIValueTable.validate(df)

    prev_q = text(
        f"""
        SELECT count(*) AS matches_found
        FROM nvi.{SURVEY_VALUES_TABLE}
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

    validated.to_sql(
        SURVEY_VALUES_TABLE, db_engine, 
        schema="nvi", index=False, if_exists="append"
    )


    df = pd.read_csv(WORKING_DIR / "output" / "acs_context_indicators_tall.csv")

    df.to_sql(
        CONTEXT_VALUES_TABLE, db_engine, 
        schema="nvi", index=False, if_exists="append"
    )