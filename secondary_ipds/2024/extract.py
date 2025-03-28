from collections import defaultdict
from sqlalchemy import text
import pandas as pd
from nvi_etl import working_dir, db_engine


WORKING_DIR = working_dir(__file__)


def extract_foreclosures(logger):
    pass


def extract_from_queries(logger):
    qs = [
        'bld_permits_citywide.sql',
        'bld_permits_districts.sql',
        'bld_permits_zones.sql',
        'blight_citywide.sql',
        'blight_districts.sql',
        'blight_zones.sql',
    ]

    result = defaultdict(list) 
    for filename in qs:
        logger.info(f"Running query '{filename}'.")

        path = WORKING_DIR / "sql" / filename

        # Get the stem from the path (basically just the final filename without the '.csv')
        stem = path.stem

        # Clip off the 'geo_type'
        *title, _ = stem.split("_")

        query = text(path.read_text())
        table = pd.read_sql(query, db_engine)

        # Add the file to the list labeled with the dataset
        result["_".join(title)].append(table)

    combined_topics = []
    for clipped_stem, files in result.items():
        file = pd.concat(files)
        combined_topics.append(file.astype({"geography": "str"}).set_index(["geo_type", "geography"]))

    wide_format = pd.concat(combined_topics, axis=1)
    wide_format.to_csv(WORKING_DIR / "input" / "ipds_wide_from_queries.csv")