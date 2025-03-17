import configparser
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np


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

