"""MSC (Miscellaneous Secondary Context) data -- births, crashes, crime, CDO, redlining."""

import configparser
from collections import defaultdict
from datetime import date

import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine, text

from nvi_etl.config import CONF_DIR, SQL_DIR
from nvi_etl.db import get_engine
from nvi_etl.registry import task, TaskResult
from nvi_etl.reshape import elongate, liquefy
from nvi_etl.aggregations import compile_indicators
from nvi_etl.geo import pin_location, pull_city_boundary, pull_council_districts, pull_zones
from nvi_etl.upsert import upsert_values, upsert_context_values

DATA_YEAR = 2025
BIRTHS_YEAR = 2024
GEOM_DATE = date(2026, 1, 1)

TABLE_MAP = {
    "crash_table": "semcog_crash_20250317",
    "cdo_table": "shp.becdd_47cdoserviceareas_20220815",
    "crime_table": "rms_crime_20260108",
    "population_table": "public.b01003_moe",
    "redlining_table": "nvi.holc_maps",
    "city_boundary_table": "shp.detroit_city_boundary_01182023",
    "neighborhood_zones_table": "nvi.neighborhood_zones",
    "council_district_table": "nvi.detroit_council_districts",
}

QUERY_FILES = [
    ("auto_crash_combined.sql", "data"),
    ("cdo_service_area_combined.sql", "data"),
    ("ped_bike_crash_combined.sql", "data"),
    ("violent_crime_all.sql", "data"),
    ("redlining_all.sql", "data"),
]


def _aggregate_births(births_gdf, geo_layer, group_col, geo_type, source):
    """Aggregate births by a geography layer."""
    births_crs = births_gdf.to_crs(geo_layer.crs)
    joined = gpd.sjoin(births_crs, geo_layer, how="left", predicate="within")

    total = joined.groupby(group_col)["KESSNER"].count().reset_index()
    total.columns = ["geography", "total_births"]
    total["geo_type"] = geo_type

    kessner_1 = joined[joined["KESSNER"] == 1]
    adequate = kessner_1.groupby(group_col)["KESSNER"].count().reset_index()
    adequate.columns = ["geography", "kessner_1_count"]

    merged = total.merge(adequate, on="geography", how="left")
    merged["percentage_adequate"] = (
        100 * merged["kessner_1_count"] / merged["total_births"]
    )
    return merged


def _extract_from_queries(logger):
    """Run SQL queries and combine into wide format."""
    params = {"data_year": DATA_YEAR, "geom_date": GEOM_DATE}

    result = defaultdict(list)
    for filename, db in QUERY_FILES:
        logger.info(f"Running '{filename}'.")
        sql_text = (SQL_DIR / filename).read_text().format(**TABLE_MAP)
        query = text(sql_text)
        table = pd.read_sql(query, get_engine(db), params=params)

        stem = (SQL_DIR / filename).stem
        *title, _ = stem.split("_")
        result["_".join(title)].append(table)

    combined_topics = []
    for _, files in result.items():
        file = pd.concat(files).astype({"geography": "str"}).set_index(["geo_type", "geography"])
        combined_topics.append(file)

    return pd.concat(combined_topics, axis=1).assign(year=DATA_YEAR).reset_index()


def _transform_births(source, logger):
    """Transform births data with spatial joins."""
    config = configparser.ConfigParser()
    config.read(CONF_DIR / "msc" / ".conf")
    data_path = config.get("nvi_2024_config", "data_extract_path")

    births_df = pd.read_csv(data_path, low_memory=False)
    births_gdf = gpd.GeoDataFrame(
        births_df,
        geometry=gpd.points_from_xy(births_df.LONGITUDE, births_df.LATITUDE),
        crs="EPSG:4326",
    )

    city_boundary = pull_city_boundary(source)
    cds = pull_council_districts(source, 2026)
    nvi_zones = pull_zones(source, 2026)

    city_wide = _aggregate_births(births_gdf, city_boundary, "geoid", "citywide", source)
    city_wide["geography"] = "Detroit"

    districts = _aggregate_births(births_gdf, cds, "district_number", "district", source)
    zones = _aggregate_births(
        births_gdf, nvi_zones, "zone_id", "zone", source
    )

    wide = pd.concat([city_wide, districts, zones])
    wide["location_id"] = wide.apply(pin_location, axis=1)

    tall = liquefy(wide)
    tall["year"] = BIRTHS_YEAR
    tall["value_type_id"] = 1
    tall["survey_id"] = 1
    return tall


@task("msc", phase=2, description="Births, crashes, crime, CDO coverage, redlining")
def run(source: Engine, target: Engine) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    primary_indicators = pd.read_csv(CONF_DIR / "msc" / "primary_indicator_ids.csv")
    context_indicators = pd.read_csv(CONF_DIR / "msc" / "context_indicator_ids.csv")
    total_rows = 0

    # Query-based indicators
    msc_wide = _extract_from_queries(logger)
    msc_wide["location_id"] = msc_wide.apply(pin_location, axis=1)
    msc_wide["year"] = DATA_YEAR

    query_tall = (
        elongate(msc_wide)
        .merge(primary_indicators, on=["indicator", "year"], how="left")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .assign(value_type_id=1, survey_id=1)
    )

    total_rows += upsert_values(target, query_tall)

    # Context indicators from queries
    indicators = compile_indicators(context_indicators, logger)
    context_wide = msc_wide.assign(**indicators)

    context_tall = (
        elongate(context_wide)
        .merge(context_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
    )
    total_rows += upsert_context_values(target, context_tall)

    # Births
    try:
        births_tall = _transform_births(source, logger)
        total_rows += upsert_values(target, births_tall)
    except Exception as e:
        logger.warning(f"Births processing failed (may need source file): {e}")

    return TaskResult(task_name="msc", rows_inserted=total_rows, success=True)
