"""
process_ipds.py -- update SET UP block for new data
"""

from pathlib import Path
from datetime import date
import configparser
import pandas as pd
import geopandas as gpd
from shapely import wkb
from sqlalchemy import text
from collections import defaultdict

from nvi_etl import working_dir, make_engine_for, setup_logging
from nvi_etl.utilities import fix_parcel_id
from nvi_etl.geo_reference import pull_zones, pull_council_districts, pin_location
from nvi_etl.reshape import elongate
from nvi_etl.schema import NVIValueTable
from nvi_etl.destinations import SURVEY_VALUES_TABLE

from d3census import variable, Geography, create_geography, create_edition, build_profile


# =============================================================================
# SET UP
# =============================================================================

WORKING_DIR = working_dir(__file__)

DATA_YEAR = 2024
BLIGHT_YEAR = 2023 
GEOM_DATE = date(2026, 1, 1)  

PARAMS = {
    "data_year": DATA_YEAR,
    "blight_year": BLIGHT_YEAR,
    "geom_date": GEOM_DATE
}

TABLE_MAP = {
    "{parcel_table}": "raw.detodp_assessors_20260131",
    "{parcel_det_table}":"raw.detodp_assessor_20260131_det",
    "{building_file_table}": "raw.building_file_20230313_2",
    "{blight_table}":"raw.detodp_blight_violations_20260131",
    "{mcm_table}":"raw.survey_mcm_2014",
    "{prop_conditions_table}":"msc.nvi_prop_conditions_2025", #TODO: Update
    "{valassis_1}": "raw.valassis_vnefplus_mi_2025_qrt4_det", 
    "{valassis_2}": "raw.valassis_vnefplus_mi_2025_qrt3_det",
    "{valassis_3}": "raw.valassis_vnefplus_mi_20250501_det",
    "{valassis_4}": "raw.valassis_vnefplus_mi_20250122_det",
    "{valassis_5}": "raw.valassis_vnefplus_mi_20241017_det",
    "{valassis_6}": "raw.valassis_vnefplus_mi_20240711_det",
    "{valassis_7}": "raw.valassis_vnefplus_mi_20240411_det",
    "{valassis_8}": "raw.valassis_vnefplus_mi_20240116_det",
    "{building_permits_table}": "raw.detodp_building_permits_20260202",
}

QUERY_FILES = [
    "building_permits.sql",
    "blight.sql",
    "pop_density.sql",
    "sq_mi.sql",
    "land_use.sql",
    "building_vacancy.sql",
    "parcel_vacancy.sql",
    "foreclosures_history.sql",
]


def _load_sql(filename: str) -> text:
  
    raw = (WORKING_DIR / "sql" / filename).read_text()
    for placeholder, table in TABLE_MAP.items():
        if isinstance(table, dict):
            for sub_placeholder, sub_table in table.items():
                raw = raw.replace(sub_placeholder, sub_table)
        else:
            raw = raw.replace(placeholder, table)
    return text(raw)


EVERYTHING = lambda _: "Detroit"  


@variable
def b01003001(geo: Geography):
    return geo.B01003._001E


def load_in_population_reference(logger):
    db_engine = make_engine_for("ipds")
    logger.info("Checking for population reference table.")

    try:
        with db_engine.connect() as db:
            row = db.execute(text("SELECT COUNT(*) FROM nvi.b01003_moe;")).fetchone() #TODO: Update to pull 2024 acs
        logger.info(f"'nvi.b01003_moe' already exists with {row.count} rows.")

    except Exception:
        logger.info("Table not found — pulling B01003 from Census API.")

        pop_table_tracts = build_profile(
            variables=[b01003001],
            geographies=[create_geography(state="26", county="163", tract="*")],
            edition=create_edition("acs5", 2024),
        )
        pop_table_county_sub = build_profile(
            variables=[b01003001],
            geographies=[create_geography(state="26", county="163", county_subdivision="22000")],
            edition=create_edition("acs5", 2024,
        )

        pd.concat([pop_table_tracts, pop_table_county_sub]).to_sql(
            "b01003_moe", db_engine, schema="nvi", if_exists="replace"
        )
        logger.info("Population table loaded successfully.")


def load_in_location_types(logger):
    logger.info("Loading parcel use key.")
    parcel_use = pd.read_csv(WORKING_DIR / "conf" / "parcel_use_codes.csv")
    parcel_use.to_sql(
        "parcel_use_summary", make_engine_for("ipds"), schema="nvi", if_exists="replace"
    )


def create_intermediate_table(logger):
    db_engine = make_engine_for("ipds")
    table_name = f"msc.nvi_prop_conditions_{DATA_YEAR}" 
    logger.info(f"Checking for intermediate table '{table_name}'.")

    try:
        with db_engine.connect() as db:
            row = db.execute(text(f"SELECT COUNT(*) FROM {table_name};")).fetchone()
        logger.info(f"'{table_name}' already exists with {row.count} rows.")

    except Exception:
        logger.info(f"Creating '{table_name}'.")
        create_q = _load_sql("create_table_nvi_prop_conditions.sql")
        with db_engine.connect() as db:
            db.execute(create_q)


# =============================================================================
# EXTRACT
# =============================================================================

def extract_foreclosures(logger):
    logger.info("Extracting foreclosures.")

    config = configparser.ConfigParser()
    config.read(WORKING_DIR / "conf" / ".conf")

    sql = text("SELECT * FROM {parcel_table}".text.replace("{parcel_table}", TABLE_MAP["{parcel_table}"])

    parcels = gpd.read_postgis(
      sql,
      db_engine,
      geom_col='geom',
      crs="EPSG:4326"
    ).to_crs(2898)
  
    nvi_zones = pull_zones(GEOM_DATE.year)
    council_districts = pull_council_districts(GEOM_DATE.year)

    tax_foreclosures = (
        pd.read_csv(config["source_files"]["foreclosures_file"])
        .query("CITY == 'DETROIT'")
        .dropna(subset=["PARCEL_ID"])
        .astype({"PARCEL_ID": "str"})
        .assign(parcel_num=lambda df: df["PARCEL_ID"].apply(fix_parcel_id))
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
        ("zone", "zone_id"), ]

    (
        pd.concat([
            stamped
            .groupby(strat)
            .aggregate(
                count_non_foreclosures=("not_in_foreclosure", "sum"),
                universe_non_foreclosures=("not_in_foreclosure", "size"),
            )
            .assign(
                percentage_non_foreclosures=calc_foreclosure_pct,
                geo_type=geo_type,
                year=DATA_YEAR,
            )
            for geo_type, strat in group_strategies
        ])
        .reset_index()
        .rename(columns={"index": "geography"})
        .to_csv(WORKING_DIR / "input" / "foreclosures_wide.csv", index=False)
    )


def extract_from_queries(logger):
    db_engine = make_engine_for("ipds")
    combined_topics = []

    for filename in QUERY_FILES:
        logger.info(f"Running query '{filename}'.")
        table = pd.read_sql(_load_sql(filename), db_engine, params=PARAMS)

        if "year" not in table.columns:
            table["year"] = DATA_YEAR

        combined_topics.append(
            table
            .astype({"geography": "str"})
            .set_index(["geo_type", "geography", "year"])
        )

    pd.concat(combined_topics, axis=1).to_csv(
        WORKING_DIR / "input" / "ipds_wide_from_queries.csv"
    )


# =============================================================================
# TRANSFORM
# =============================================================================

def _read_location_pinned_file():
    return (
        pd.read_csv(WORKING_DIR / "input" / "ipds_wide_from_queries.csv")
        .assign(location_id=lambda df: df.apply(pin_location, axis=1))
    )


def _build_pct_calculator(indicator, cent_scale=False):
    def inner(df):
        numerator = df[f"count_{indicator}"] * (100 if cent_scale else 1)
        denominator = df[f"universe_{indicator}"]
        return numerator / denominator  
    return inner


def _compile_indicator_aggregations(indicators: pd.DataFrame, logger) -> dict:
    aggregations = {}
    for _, indicator in indicators.iterrows():
        if indicator["indicator_type"] == "count":
            continue 
        elif indicator["indicator_type"] in {"percentage", "rate"}:
            fn = _build_pct_calculator(indicator["indicator"])
        else:
            logger.error(
                f"'{indicator['indicator_type']}' is not a valid indicator type. "
                f"'{indicator['indicator']}': SKIPPING"
            )
            continue

        aggregations[f"{indicator['indicator_type']}_{indicator['indicator']}"] = fn

    return aggregations


def transform_foreclosures(primary_indicators, logger):
    logger.info("Transforming foreclosures into tall format.")

    wide_file = (
        pd.read_csv(WORKING_DIR / "input" / "foreclosures_wide.csv")
        .assign(location_id=lambda df: df.apply(pin_location, axis=1))
    )

    (
        elongate(wide_file)
        .merge(primary_indicators, on=["indicator", "year"], how="left")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .assign(value_type_id=1)
        .to_csv(WORKING_DIR / "output" / "foreclosures_tall.csv", index=False)
    )


def transform_from_queries(primary_indicators, logger):
    logger.info("Transforming IPDS query output into tall format.")

    wide_file = _read_location_pinned_file()

    (
        elongate(wide_file)
        .merge(primary_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .assign(value_type_id=1)
        .to_csv(WORKING_DIR / "output" / "ipds_primary_tall_from_queries.csv", index=False)
    )


def transform_primary(logger):
    primary_indicators = pd.read_csv(WORKING_DIR / "conf" / "primary_indicator_ids.csv")
    transform_foreclosures(primary_indicators, logger)
    transform_from_queries(primary_indicators, logger)


def transform_context(logger):
    logger.info("Transforming IPDS context indicators.")

    context_indicators = pd.read_csv(WORKING_DIR / "conf" / "context_indicator_ids.csv")
    aggregations = _compile_indicator_aggregations(context_indicators, logger)

    wide_file = _read_location_pinned_file().assign(**aggregations)

    (
        elongate(wide_file)
        .merge(context_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .to_csv(WORKING_DIR / "output" / "ipds_context_tall_from_queries.csv", index=False)
    )


# =============================================================================
# LOAD
# =============================================================================

def load_from_queries(logger):
    logger.warning("Loading IPDS query data into context values table.")
    db_engine = make_engine_for("data")
    file = pd.read_csv(WORKING_DIR / "output" / "ipds_tall_from_queries.csv")
    NVIValueTable.validate(file).to_sql(
        SURVEY_VALUES_TABLE, db_engine, schema="nvi", index=False, if_exists="append"
    )


def load_foreclosures(logger):
    logger.info("Loading foreclosures into context values table.")
    db_engine = make_engine_for("data")
    file = pd.read_csv(WORKING_DIR / "output" / "foreclosures_tall.csv")
    NVIValueTable.validate(file).to_sql(
        SURVEY_VALUES_TABLE, db_engine, schema="nvi", index=False, if_exists="append"
    )


# =============================================================================
# MAIN
# =============================================================================

def main():
    logger = setup_logging()

    load_in_population_reference(logger)
    load_in_location_types(logger)
    create_intermediate_table(logger)

    # EXTRACT
    extract_foreclosures(logger)
    extract_from_queries(logger)

    # TRANSFORM
    transform_primary(logger)
    transform_context(logger)

    # LOAD
    # load_from_queries(logger)
    # load_foreclosures(logger)


if __name__ == "__main__":
    main()
