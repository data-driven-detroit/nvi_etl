from nvi_etl import extract_from_sql_file, db_engine, working_dir
from sqlalchemy import text


WORKING_DIR = working_dir(__file__)


def create_itermediate_table(logger):
    logger.info("Creating 'nvi_prop_conditions_2025' table if it doesn't exist")

    q = text(
        (
            WORKING_DIR / "sql" / "create_table_nvi_prop_conditions.sql"
        ).read_text()
    )

    # TODO Error handling if this breaks somehow.
    with db_engine.connect() as db:
        response = db.execute(q)


def extract_ipds(logger):
    logger.info("Extract function for ipds not written yet!")
