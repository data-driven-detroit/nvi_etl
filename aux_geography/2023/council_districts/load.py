from pathlib import Path
import geopandas as gpd


WORKING_DIR = Path(__file__).resolve().parent


def load_council_districts(logger):
    logger.info("Loading council districts to database.")
    file = gpd.read_file(
        WORKING_DIR / "output" / "council_districts_2014_validated.geojson",
    )

    correct_types = DetroitCouncilDistricts.validate(file)

    correct_types.to_postgis("detroit_council_districts", db_engine, schema="nvi", if_exists="append")


