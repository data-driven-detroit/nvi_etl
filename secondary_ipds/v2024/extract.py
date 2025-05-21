import configparser
from nvi_etl.utilities import fix_parcel_id
from nvi_etl.geo_reference import pull_zones, pull_council_districts
from collections import defaultdict
from sqlalchemy import text
import pandas as pd
import geopandas as gpd
from shapely import wkb
from nvi_etl import working_dir, make_engine_for


WORKING_DIR = working_dir(__file__)

EVERYTHING = lambda _: "Detroit" # This is used later to take entire df as a single group


def extract_foreclosures(logger):
    logger.info("Extracting forclosures from various files and combining phase one.")

    config = configparser.ConfigParser()
    config.read(WORKING_DIR / "conf" / ".conf")

    # Load the various geometry files
    # FIXME -- pull this from the database as native geopandas
    parcels = (
        pd.read_csv(config["source_files"]["assessors_file"], low_memory=False)
        .assign(
            geometry=lambda df: df["geom"].apply(wkb.loads)
        )
    )

    # Convert to a GeoDataFrame
    parcels = (
        gpd.GeoDataFrame(parcels, geometry="geometry", crs="EPSG:4326")
        .drop(columns=["geom"])
        .to_crs(2898)
    )

    nvi_zones = pull_zones(2026)
    council_districts = pull_council_districts(2026)


    # Load the main dataset and conduct some clean-up
    tax_foreclosures = (
        pd.read_csv(config["source_files"]["foreclosures_file"])
        .query("CITY == 'DETROIT'")
        .dropna(subset=["PARCEL_ID"])
        .astype({"PARCEL_ID": "str"})
        .assign(
            parcel_num=lambda df: df["PARCEL_ID"].apply(fix_parcel_id)
        )
    )

    stamped = (
        parcels
        .merge(tax_foreclosures, on="parcel_num", how="left")
        .assign(not_in_foreclosure=lambda df: df["PARCEL_ID"].isna())
        .sjoin(council_districts[["district_number", "geometry"]], predicate="within", how="left")
        .drop("index_right", axis=1)
        .sjoin(nvi_zones[["zone_id", "geometry"]], predicate="within", how="left")
        .drop("index_right", axis=1)
    )


    def calc_foreclosure_pct(df):
        return df["count_non_foreclosures"] / df["universe_non_foreclosures"]

    group_strategies = [
        ("citywide", EVERYTHING), 
        ("district", "district_number"), 
        ("zone", "zone_id")
    ]

    (
        pd.concat(
            [
                stamped
                .groupby(strat)
                .aggregate(
                    count_non_foreclosures=("not_in_foreclosure", "sum"),
                    universe_non_foreclosures=("not_in_foreclosure", "size"),
                )
                .assign(
                    percentage_non_foreclosures=calc_foreclosure_pct,
                    geo_type=geo_type,
                    year=2024
                )
                for geo_type, strat in group_strategies
            ]
        )
        .reset_index()
        .rename(columns={"index": "geography"})
        .to_csv(WORKING_DIR / "input" / "foreclosures_wide.csv", index=False)
    )


def extract_from_queries(logger):
    db_engine = make_engine_for("ipds")
    qs = [
        'bld_permits_citywide.sql',
        'bld_permits_districts.sql',
        'bld_permits_zones.sql',
        'blight_citywide.sql',
        'blight_districts.sql',
        'blight_zones.sql',
        'pop_density.sql',
        'sq_mi.sql',
        'land_use.sql',
        'parcel_vacancy.sql',
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

    # FIXME -- see the later added sql files on how to run the aggregations
    # without requiring separate files and this intermediate vertical concatenation
    for clipped_stem, files in result.items():
        file = pd.concat(files)
        combined_topics.append(file.astype({"geography": "str"}).set_index(["geo_type", "geography"]))

    wide_format = pd.concat(combined_topics, axis=1).assign(year=2024)
    wide_format.to_csv(WORKING_DIR / "input" / "ipds_wide_from_queries.csv")


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    extract_foreclosures(logger)
    extract_from_queries(logger)