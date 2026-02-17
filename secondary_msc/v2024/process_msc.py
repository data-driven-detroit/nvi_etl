import configparser
import geopandas as gpd
import pandas as pd

from datetime import date
from collections import defaultdict
from pathlib import Path
from sqlalchemy import text


from nvi_etl import working_dir, db_engine, setup_logging
from nvi_etl import liquefy
from nvi_etl.reshape import elongate
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import CONTEXT_VALUES_TABLE, SURVEY_VALUES_TABLE
from nvi_etl.geo_reference import (
    pin_location,
    pull_city_boundary,
    pull_council_districts,
    pull_zones,
)


DATA_YEAR = '2024'  
GEOM_DATE = date(2026, 1, 1)
TABLE_MAP = {
    "{crash_table}": "semcog_crash_20250317",
    "{cdo_table}": "shp.becdd_47cdoserviceareas_20220815",
    "{crime_table}": "rms_crime_20260108",
    "{population_table}": "public.b01003_moe",
    "{redlining_table}": "nvi.holc_maps",
    "{city_boundary_table}": "shp.detroit_city_boundary_01182023",
    "{neighborhood_zones_table}": "nvi.neighborhood_zones",
    "{council_district_table}": "nvi.detroit_council_districts"
}

WORKING_DIR = Path(__file__).resolve().parent



"""
Extract:
"""
def extract_births(logger):
    logger.info("No extraction necessary for births file--reading directly from source.")
    logger.info("Extracting births data.")

    output_path = WORKING_DIR / "input" / "births_extracted_{DATA_YEAR}.geojson"

    if output_path.exists():
        logger.info("Births already extracted, skipping to transform.")
        return 

    parser = configparser.ConfigParser()
    parser.read(WORKING_DIR / "conf" / ".conf")
    data_extract_path = parser.get('nvi_2024_config', 'data_extract_path')

    if Path(data_extract_path).exists():
        logger.info("File has already been extracted--continuing.")
        return 0

    births_df = pd.read_csv(data_extract_path, low_memory=False)

    # Convert to a GeoDataFrame
    births_gdf = gpd.GeoDataFrame(
        births_df,
        geometry=gpd.points_from_xy(births_df.LONGITUDE, births_df.LATITUDE),
        crs="EPSG:4326",
    )

    # FIXME: We can save this temporarily locally -- right?

    births_gdf.to_file(output_path)




def extract_from_queries(logger):
    logger.warning("Extracting data based on sql query files.")

    params = {
        "data_year": DATA_YEAR,
        "geom_date": GEOM_DATE
    }

    filenames = [
        "auto_crash_combined.sql",
        "cdo_service_area_combined.sql",
        "ped_bike_crash_combined.sql",
        "violent_crime_all.sql",
        "redlining_all.sql",
    ]

    result = defaultdict(list) 
    for filename in filenames:
        logger.info(f"Running '{filename}'.")

        path = WORKING_DIR / "sql" / filename

        sql_text = path.read_text()
        for placeholder, table_name in TABLE_MAP.items():
            sql_text = sql_text.replace(placeholder, table_name)

        # Get the stem from the path (basically just the final filename without the '.csv')
        stem = path.stem

        # Clip off the 'geo_type'
        *title, _ = stem.split("_")

        query = text(path.read_text())
        table = pd.read_sql(query, db_engine, params=params)

        # Add the file to the list labeled with the dataset
        result["_".join(title)].append(table)

    combined_topics = []
    for clipped_stem, files in result.items():
        file = pd.concat(files).astype({"geography": "str"}).set_index(["geo_type", "geography"])
        combined_topics.append(file)

    wide_format = pd.concat(combined_topics, axis=1).assign(year=DATA_YEAR)
    wide_format.to_csv(WORKING_DIR / "input" / "msc_wide_{DATA_YEAR}_from_queries.csv")




"""
Transform:
"""

from aggregations import compile_indicators

def aggregate_city_wide(births_gdf, logger):
    city_boundary = pull_city_boundary()

    # Ensure both GeoDataFrames have the same CRS
    births_gdf = births_gdf.to_crs(city_boundary.crs)

    # Perform a spatial join to assign births to geographic areas
    births_with_geography = gpd.sjoin(
        births_gdf, city_boundary, how="left", predicate="within"
    )

    # No of rows in the data
    total_births = (
        births_with_geography.groupby("geoid")["KESSNER"].count().reset_index()
    )
    total_births.columns = ["geography", "total_births"]

    total_births["geo_type"] = "citywide"
    total_births["geography"] = "Detroit"

    # Filter for births where kesser == 1
    births_kesser_1 = births_with_geography[
        births_with_geography["KESSNER"] == 1
    ]

    # Aggregate count of kesser = 1 by geographic area (e.g., 'zone' column from shapefile)
    adequate_care_counts = (
        births_kesser_1.groupby("geoid")["KESSNER"].count().reset_index()
    )
    adequate_care_counts.columns = ["geography", "kessner_1_count"]
    adequate_care_counts["geography"] = "Detroit"

    births_summary = total_births.merge(
        adequate_care_counts, on="geography", how="left"
    )
    births_summary["percentage_adequate"] = (
        births_summary["kessner_1_count"] / births_summary["total_births"]
    )

    return births_summary


def aggregate_to_cds(births_gdf, logger):
    cds = pull_council_districts(2026)
    # Ensure both GeoDataFrames have the same CRS
    births_gdf_cd = births_gdf.to_crs(cds.crs)

    # Perform a spatial join to assign births to geographic areas
    births_with_cd = gpd.sjoin(
        births_gdf_cd, cds, how="left", predicate="within"
    )

    # No of rows in the data
    total_births_cd = (
        births_with_cd.groupby("district_number")["KESSNER"].count().reset_index()
    )
    total_births_cd.columns = ["geography", "total_births"]
    total_births_cd["geo_type"] = "district"

    # Filter for births where kesser == 1
    births_kesser_1_cd = births_with_cd[births_with_cd["KESSNER"] == 1]

    # Aggregate count of kesser = 1 by geographic area (e.g., 'zone' column from shapefile)
    adequate_care_counts_cd = (
        births_kesser_1_cd.groupby("district_number")["KESSNER"]
        .count()
        .reset_index()
    )
    adequate_care_counts_cd.columns = ["geography", "kessner_1_count"]

    births_summary_cd = total_births_cd.merge(
        adequate_care_counts_cd, on="geography", how="left"
    )
    births_summary_cd["percentage_adequate"] = (
        births_summary_cd["kessner_1_count"] / births_summary_cd["total_births"]
    )

    return births_summary_cd


def aggregate_to_zones(births_gdf, logger):
    nvi_zones = pull_zones(2026)

    # Ensure both GeoDataFrames have the same CRS
    births_gdf = births_gdf.to_crs(nvi_zones.crs)

    # Spatial join: Assign each point to a polygon
    merged_gdf = gpd.sjoin(
        births_gdf, nvi_zones, how="left", predicate="intersects"
    )

    # No of rows in the data
    total_births_nvi = (
        merged_gdf.groupby("zone_id")["KESSNER"]
        .count()
        .reset_index()
    )
    total_births_nvi.columns = ["geography", "total_births"]
    total_births_nvi["geo_type"] = "zone"

    # Filter for births where kesser == 1
    births_kesser_1_nvi = merged_gdf[merged_gdf["KESSNER"] == 1]

    # Aggregate count of kesser = 1 by geographic area (e.g., 'zone' column from shapefile)
    adequate_care_counts_nvi = (
        births_kesser_1_nvi.groupby("zone_id")["KESSNER"]
        .count()
        .reset_index()
    )
    adequate_care_counts_nvi.columns = [
        "geography",
        "kessner_1_count",
    ]

    births_summary_nvi = total_births_nvi.merge(
        adequate_care_counts_nvi, on="geography", how="left"
    )
    births_summary_nvi["percentage_adequate"] = (
        births_summary_nvi["kessner_1_count"]
        / births_summary_nvi["total_births"]
    )

    return births_summary_nvi


def transform_births(logger):
    logger.info("Transforming births.")

    births_gdf = gpd.read_file(
        WORKING_DIR / "input" / "births_extracted_2023.geojson"
    )

    # TODO: Combine these with appropriate location_ids and save

    city_wide = aggregate_city_wide(births_gdf, logger)
    council_districts = aggregate_to_cds(births_gdf, logger)
    nvi_zones = aggregate_to_zones(births_gdf, logger)

    # Save each of these
    
    wide_format = pd.concat([
        city_wide,
        council_districts,
        nvi_zones,
    ])

    wide_format.to_csv(WORKING_DIR / "output" / "births_output_wide.csv")
    wide_format["location_id"] = wide_format.apply(pin_location, axis=1)

    tall_format = liquefy(wide_format)
    tall_format["year"] = 2024 # FIXME
    tall_format["value_type_id"] = 1
    tall_format.to_csv(WORKING_DIR / "output" / "births_output_tall.csv", index=False)


def transform_from_queries(logger):
    logger.info("Transforming output from MSC queries.")

    primary_indicators = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv")

    msc_wide = pd.read_csv(WORKING_DIR / "input" / "msc_wide_from_queries.csv")
    msc_wide["location_id"] = msc_wide.apply(pin_location, axis=1)
    msc_wide["year"] = 2024

    melted = (
        pd.wide_to_long(
            msc_wide,
            stubnames=["count", "universe", "percentage", "rate", "per", "dollars", "index"],
            i=["location_id", "year"],
            j="indicator",
            sep="_",
            suffix=".*",
        )
        .reset_index()
        .rename(columns={"per": "rate_per"})
        .merge(primary_indicators, on=["indicator", "year"], how="left")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .assign(value_type_id=1)
    )

    melted.to_csv(WORKING_DIR / "output" / "msc_output_tall.csv", index=False)


def read_location_pinned_file():
    return (
        pd.read_csv(WORKING_DIR / "input" / "msc_wide_from_queries.csv")
        .assign(
            location_id=lambda df: df.apply(pin_location, axis=1)
        )
    )


def transform_context(logger):
    logger.info("Transforming MSC context indicators.")

    context_indicators = pd.read_csv(WORKING_DIR / "conf" / "context_indicator_ids.csv")
    indicators = compile_indicators(context_indicators, logger)

    wide_file = read_location_pinned_file().assign(**indicators)

    tall_file = (
        elongate(wide_file)
        .merge(context_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
    )

    tall_file.to_csv(WORKING_DIR / "output" / "msc_context_tall_from_queries.csv", index=False)




"""
LOAD
"""
def load_births(logger):
    logger.warning("Loading births data to context values table.")

    file = pd.read_csv(WORKING_DIR / "output" / "births_output_tall.csv")

    validated = NVIValueTable.validate(file)

    validated.to_sql(SURVEY_VALUES_TABLE, db_engine, schema="nvi", index=False, if_exists="append")



def load_from_queries(logger):
    logger.warning("Loading other msc data into context values table.")

    file = pd.read_csv(WORKING_DIR / "output" / "msc_output_tall.csv")

    validated = NVIValueTable.validate(file)

    validated.to_sql(SURVEY_VALUES_TABLE, db_engine, schema="nvi", index=False, if_exists="append")




"""
Process
"""


def main():
    logger = setup_logging()


    logger.info("Starting births")
    extract_births(logger)
    # load_births(logger)


    logger.info("Starting pulls from queries")
    extract_from_queries(logger)
    # load_from_queries(logger)

    transform_births(logger)
    transform_from_queries(logger)
    transform_context(logger)

if __name__ == "__main__":
    main()
