from nvi_etl import working_dir, make_engine_for
import pandas as pd
from sqlalchemy import text
from d3census import (
    variable,
    Geography,
    create_geography,
    create_edition,
    build_profile,
)


WORKING_DIR = working_dir(__file__)


@variable
def b01003001(geo: Geography):
    return geo.B01003._001E


def load_in_population_reference(logger):

    db_engine = make_engine_for("ipds")
    logger.info("Creating population reference table if it doesn't already exist.")
    try:
        test_q = text("SELECT COUNT(*) FROM nvi.b01003_moe;")

        with db_engine.connect() as db:
            response = db.execute(test_q)
            row = response.fetchone()
        
        logger.info(
            f"'nvi.b01003_moe' already exists with {row.count} rows." # type: ignore
        )

    except: #FIXME Bear except
        logger.info(f"No table exists. Pulling b01003 from Census API.")

        pop_table_tracts = build_profile(
            variables=[
                b01003001
            ],
            geographies=[
                create_geography(state="26", county="163", tract="*"), # All tracts in Wayne County
            ],
            edition=create_edition("acs5", 2023)
        )

        pop_table_county_sub = build_profile(
            variables=[
                b01003001
            ],
            geographies=[
                create_geography(state="26", county="163", county_subdivision="22000"), # Detroit
            ],
            edition=create_edition("acs5", 2023)
        )

        pd.concat(
            [pop_table_tracts, pop_table_county_sub]
        ).to_sql("b01003_moe", db_engine, schema="nvi", if_exists="replace")

        logger.info("Population table loaded successfully!")



def create_itermediate_table(logger):
    db_engine = make_engine_for("ipds")
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
    except: #FIXME Bear except
        create_q = text(
            (
                WORKING_DIR / "sql" / "create_table_nvi_prop_conditions.sql"
            ).read_text()
        )

        # TODO Error handling if this breaks somehow.
        with db_engine.connect() as db:
            response = db.execute(create_q)


