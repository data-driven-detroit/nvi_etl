import configparser
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np

# nvi_shp = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/NVI Zones/nvi_neighborhood_zones_temp_2025.shp")
# detroit_shp = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/City_of_Detroit_Boundary/City_of_Detroit_Boundary.shp")
# cd_shp = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/Detroit_City_Council_Districts_2026/Detroit_City_Council_Districts_2026.shp")

# Load birth data from csv


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

    births_gdf.to_file(WORKING_DIR / "output" / "births_extracted_2023.geojson")


def extract_rms_crime(logger):
    logger.warning("RMS Crime extract hasn't been written beyond the SQL!")


def extract_cdo_coverage(logger):
    logger.warning("CDO Coverage extract hasn't been written beyond the SQL!")
