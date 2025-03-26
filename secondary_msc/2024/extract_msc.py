import configparser
from pathlib import Path
import geopandas as gpd
import pandas as pd
from sqlalchemy import text

from nvi_etl import db_engine


WORKING_DIR = Path(__file__).resolve().parent


def extract_births(logger):
    logger.info("Extracting births data.")

    parser = configparser.ConfigParser()
    parser.read(WORKING_DIR / "conf" / ".conf")
    data_extract_path = parser.get('nvi_2024_config', 'data_extract_path')

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
        "cdo_service_area_percent_cds.sql",
        "cdo_service_area_percent_citywide.sql",
        "cdo_service_area_percent_zones.sql",
        "ped_bike_crash_cds.sql",
        "ped_bike_crash_citywide.sql",
        "ped_bike_crash_zones.sql",
        "rms_incidents_cds.sql",
        "rms_incidents_citywide.sql",
        "rms_incidents_zones.sql",
    ]
    
    result = []
    for filename in filenames:
        query = text((WORKING_DIR / "sql" / filename).read_text())
        file = pd.read_sql(query, db_engine)
        result.append(file)

    return pd.concat(result)
