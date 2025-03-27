from nvi_etl import working_dir
import configparser
import pandas as pd
import geopandas as gpd
from shapely import wkb # FIXME Let's depend on the parcel file once it's complete


WORKING_DIR = working_dir(__file__)


conf = configparser.ConfigParser()
conf.read(WORKING_DIR / "conf" / ".conf")


def transform_foreclosures(logger):
    logger.info("Pulling foreclosures from the vault.")

    tax = pd.read_csv(conf["source_files"]["foreclosures"])
    detodp = pd.read_csv(conf["source_files"]["assessors_file"])

    # File preparation

    from shapely import wkb

    # Convert WKB to Geometry
    detodp["geometry"] = detodp["geom"].apply(wkb.loads)

    # Convert to a GeoDataFrame
    detodp_gdf = gpd.GeoDataFrame(detodp, geometry="geometry", crs="EPSG:4326")

    # Drop the old WKB column if not needed
    detodp_gdf.drop(columns=["geom"], inplace=True)

    # Zones
    # Council Districts
    # City-wide


def transform_ipds(logger):
    logger.info("Transform function for ipds not written yet!")
