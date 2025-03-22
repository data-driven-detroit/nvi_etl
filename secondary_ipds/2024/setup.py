from nvi_etl import db_engine, working_dir
from sqlalchemy import text
from sqlalchemy.exc import OperationalError


WORKING_DIR = working_dir(__file__)


def create_itermediate_table(logger):
    logger.info("Creating 'nvi_prop_conditions_2025' table if it doesn't exist")

    test_q = text("SELECT COUNT(*) FROM msc.nvi_prop_conditions_2025;")
    
    try:
        with db_engine.connect() as db:
            response = db.execute(test_q)
            
            row = response.fetchone()
        
        logger.info(
            f"'nvi_prop_conditions_2025' already exists with {row.count} rows." # type: ignore
        )

    # The if the table doesn't exist make it
    except OperationalError:
        create_q = text(
            (
                WORKING_DIR / "sql" / "create_table_nvi_prop_conditions.sql"
            ).read_text()
        )

        # TODO Error handling if this breaks somehow.
        with db_engine.connect() as db:
            response = db.execute(create_q)


