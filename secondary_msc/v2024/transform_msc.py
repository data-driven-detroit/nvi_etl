from pathlib import Path
import geopandas as gpd
import pandas as pd
from nvi_etl import liquefy
from nvi_etl.reshape import elongate
from nvi_etl.geo_reference import (
    pin_location,
    pull_city_boundary,
    pull_council_districts,
    pull_zones,
)

from aggregations import compile_indicators


WORKING_DIR = Path(__file__).resolve().parent


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


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    # transform_births(logger)
    transform_from_queries(logger)
    transform_context(logger)