from nvi_etl.utilities import fix_parcel_id
from collections import defaultdict
from sqlalchemy import text
import pandas as pd
import geopandas as gpd
from shapely import wkb
from nvi_etl import working_dir, db_engine


WORKING_DIR = working_dir(__file__)


def extract_foreclosures(logger):
    logger.info("Extracting forclosures from various files and combining phase one.")

    # FIXME This isn't really an extract -- lot of transform code here.
    tax = pd.read_csv("V:/IPDS/Wayne County Tax Foreclosures/Data/2024/Prepared/wcto_foreclosed_05092024.csv")
    detodp = pd.read_csv("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/detodp_assessor_20240205.csv")


    tax = tax[tax["CITY"] == 'DETROIT']

    # Convert WKB to Geometry
    detodp["geometry"] = detodp["geom"].apply(wkb.loads)

    # Convert to a GeoDataFrame
    detodp_gdf = gpd.GeoDataFrame(detodp, geometry="geometry", crs="EPSG:4326")

    # Drop the old WKB column if not needed
    detodp_gdf.drop(columns=["geom"], inplace=True)


    # Apply the function to the PARCEL_ID column
    tax = tax.dropna(subset=["PARCEL_ID"])
    tax["PARCEL_ID"] = tax["PARCEL_ID"].astype(str).apply(fix_parcel_id)

    citywide = (
        tax.aggregate(Detroit=("PARCEL_ID", "count"))
        .assign(geo_type="citywide", geography="Detroit", total_properties=len(detodp))
        .rename(columns={"PARCEL_ID": "num_foreclosures"})
        .assign(foreclosure_rate=lambda df: 100 * df["num_foreclosures"] / df["total_properties"])
        .reset_index().drop("index", axis=1)
    )


    nvi_shp = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/NVI Zones/nvi_neighborhood_zones_temp_2025.shp")
    detroit_shp = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/City_of_Detroit_Boundary/City_of_Detroit_Boundary.shp")
    cd_shp = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/Detroit_City_Council_Districts_2026/Detroit_City_Council_Districts_2026.shp")

    merged = pd.merge(tax, detodp, left_on="PARCEL_ID", right_on="parcel_num", how="inner")


    # Convert WKB to Geometry
    merged["geometry"] = merged["geom"].apply(wkb.loads)

    # Convert to a GeoDataFrame
    merged_gdf = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")

    # Drop the old WKB column if not needed
    merged_gdf.drop(columns=["geom"], inplace=True)

    # NVI ZONES

    merged_nvi = merged_gdf.to_crs(nvi_shp.crs)
    merged_with_nvi = gpd.sjoin(merged_nvi, nvi_shp, how="left", predicate="within")
    detodp_nvi= detodp_gdf.to_crs(nvi_shp.crs)
    detodp_with_nvi = gpd.sjoin(detodp_nvi, nvi_shp, how="left", predicate="within")

    # Total number of properties in foreclosure, grouped by district_n and zone_id
    num_foreclosures_nvi = merged_with_nvi.groupby(["zone_id"]).size().reset_index(name="num_foreclosures")

    # Total number of properties (from detodp with geography), grouped by district_n and zone_id
    total_properties_nvi = detodp_with_nvi.groupby(["zone_id"]).size().reset_index(name="total_properties")

    foreclosure_rate_nvi = num_foreclosures_nvi.merge(total_properties_nvi, on="zone_id", how="left")
    foreclosure_rate_nvi["foreclosure_rate"] = (foreclosure_rate_nvi["num_foreclosures"] / foreclosure_rate_nvi["total_properties"]) * 100

    nvi_zones = foreclosure_rate_nvi.assign(geo_type="zone").rename(columns={"zone_id": "geography"})

    ## COUNCIL DISTRICTS
    merged_cd = merged_gdf.to_crs(cd_shp.crs)
    merged_with_cd = gpd.sjoin(merged_cd, cd_shp, how="left", predicate="within")
    detodp_cd= detodp_gdf.to_crs(cd_shp.crs)
    detodp_with_cd = gpd.sjoin(detodp_cd, cd_shp, how="left", predicate="within")

    # Total number of properties in foreclosure, grouped by district_n and zone_id
    num_foreclosures_cd = merged_with_cd.groupby("district_n").size().reset_index(name="num_foreclosures")

    # Total number of properties (from detodp with geography), grouped by district_n and zone_id
    total_properties_cd = detodp_with_cd.groupby("district_n").size().reset_index(name="total_properties")

    foreclosure_rate_cd = num_foreclosures_cd.merge(total_properties_cd, on="district_n", how="left")
    foreclosure_rate_cd["foreclosure_rate"] = (foreclosure_rate_cd["num_foreclosures"] / foreclosure_rate_cd["total_properties"]) * 100

    council_districts = (
        foreclosure_rate_cd
        .assign(geo_type="district")
        .rename(columns={"district_n": "geography"})
        .astype({"geography": "int"})
        .astype({"geography": "str"})
    )

    combined = pd.concat([
        citywide,
        nvi_zones,
        council_districts,
    ])

    combined.to_csv(WORKING_DIR / "input" / "foreclosures_wide.csv")


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