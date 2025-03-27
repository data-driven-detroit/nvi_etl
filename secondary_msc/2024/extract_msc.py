import configparser
from collections import defaultdict
from pathlib import Path
import geopandas as gpd
import pandas as pd
from sqlalchemy import text

from nvi_etl import db_engine


WORKING_DIR = Path(__file__).resolve().parent


def extract_births(logger):
    logger.info("No extraction necessary for births file--reading directly from source.")
    logger.info("Extracting births data.")

    parser = configparser.ConfigParser()
    parser.read(WORKING_DIR / "conf" / ".conf")
    data_extract_path = parser.get('nvi_2024_config', 'data_extract_path')

    if Path(data_extract_path).exists():
        logger.info("File has already been extracted--continuing.")
        return 0

    births_df = pd.read_csv(data_extract_path, low_memory=False)

    # Assuming the birth data contains 'LATITUDE' and 'LONGITUDE' columns
    # Convert to a GeoDataFrame
    births_gdf = gpd.GeoDataFrame(
        births_df,
        geometry=gpd.points_from_xy(births_df.LONGITUDE, births_df.LATITUDE),
        crs="EPSG:4326",
    )

    # FIXME: We can save this temporarily locally -- right?

    births_gdf.to_file(WORKING_DIR / "input" / "births_extracted_2023.geojson")


def extract_from_queries(logger):
    logger.warning("Extracting data based on sql query files.")

    filenames = [
        "auto_crash_cds.sql",
        "auto_crash_citywide.sql",
        "auto_crash_zones.sql",
        # "cdo_service_area_percent_cds.sql",
        # "cdo_service_area_percent_citywide.sql",
        # "cdo_service_area_percent_zones.sql",
        "ped_bike_crash_cds.sql",
        "ped_bike_crash_citywide.sql",
        "ped_bike_crash_zones.sql",
        "rms_incidents_cds.sql",
        "rms_incidents_citywide.sql",
        "rms_incidents_zones.sql",
    ]

    result = defaultdict(list) 
    for filename in filenames:
        logger.info(f"Running '{filename}'.")

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
        logger.info(f"Saving '{clipped_stem}.csv'")
        file = pd.concat(files)
        combined_topics.append(file.astype({"geography": "str"}).set_index(["geo_type", "geography"]))

    wide_format = pd.concat(combined_topics, axis=1)
    wide_format.to_csv(WORKING_DIR / "input" / "msc_wide_from_queries.csv")