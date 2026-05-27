"""IPDS (Infrastructure, Property, Development & Safety) data ETL task."""

from datetime import date

import pandas as pd
from sqlalchemy import Engine, text

from nvi_etl.config import CONF_DIR, SQL_DIR, CENSUS_API_KEY
from nvi_etl.db import get_engine
from nvi_etl.registry import task, TaskResult
from nvi_etl.reshape import elongate
from nvi_etl.geo import pin_location
from nvi_etl.upsert import upsert_values, upsert_context_values


DATA_YEAR = 2025
BLIGHT_YEAR = 2023
GEOM_DATE = date(2026, 1, 1)

PARAMS = {
    "data_year": DATA_YEAR,
    "blight_year": BLIGHT_YEAR,
    "geom_date": GEOM_DATE,
}

TABLE_MAP = {
    "parcel_table": "raw.detodp_assessors_20260131",
    "parcel_det_table": "raw.detodp_assessor_20260131_det",
    "building_file_table": "raw.building_file_20230313_2",
    "blight_table": "raw.detodp_blight_violations_20260131",
    "mcm_table": "raw.survey_mcm_2014",
    "prop_conditions_table": "msc.nvi_prop_conditions_2025",
    "valassis_1": "raw.valassis_vnefplus_mi_2025_qrt4_det",
    "valassis_2": "raw.valassis_vnefplus_mi_2025_qrt3_det",
    "valassis_3": "raw.valassis_vnefplus_mi_20250501_det",
    "valassis_4": "raw.valassis_vnefplus_mi_20250122_det",
    "valassis_5": "raw.valassis_vnefplus_mi_20241017_det",
    "valassis_6": "raw.valassis_vnefplus_mi_20240711_det",
    "valassis_7": "raw.valassis_vnefplus_mi_20240411_det",
    "valassis_8": "raw.valassis_vnefplus_mi_20240116_det",
    "building_permits_table": "raw.detodp_building_permits_20260202",
}

QUERY_FILES = [
    "building_permits.sql",
    "blight.sql",
    "pop_density.sql",
    "sq_mi.sql",
    "land_use.sql",
    "building_vacancy.sql",
    "parcel_vacancy.sql",
]


def _load_sql(filename: str) -> text:
    raw = (SQL_DIR / filename).read_text()
    raw = raw.format(**TABLE_MAP)
    return text(raw)


def _setup_population_reference(ipds_engine, logger):
    """Load population reference table if it doesn't exist."""
    try:
        with ipds_engine.connect() as db:
            row = db.execute(text("SELECT COUNT(*) FROM nvi.b01003_moe;")).fetchone()
        logger.info(f"'nvi.b01003_moe' already exists with {row.count} rows.")
    except Exception:
        from d3census import variable, Geography, create_geography, create_edition, build_profile

        @variable
        def b01003001(geo: Geography):
            return geo.B01003._001E

        logger.info("Table not found -- pulling B01003 from Census API.")
        pop_tracts = build_profile(
            variables=[b01003001],
            geographies=[create_geography(state="26", county="163", tract="*")],
            edition=create_edition("acs5", 2024),
            api_key=CENSUS_API_KEY,
        )
        pop_county_sub = build_profile(
            variables=[b01003001],
            geographies=[create_geography(state="26", county="163", county_subdivision="22000")],
            edition=create_edition("acs5", 2024),
            api_key=CENSUS_API_KEY,
        )
        final = pd.concat([pop_tracts, pop_county_sub]).assign(year=2024)
        final.to_sql("b01003_moe", ipds_engine, schema="etl", if_exists="replace")


def _setup_location_types(ipds_engine, logger):
    """Load parcel use key."""
    parcel_use = pd.read_csv(CONF_DIR / "ipds" / "parcel_use_codes.csv")
    parcel_use.to_sql("parcel_use_summary", ipds_engine, schema="nvi", if_exists="replace")


def _setup_intermediate_table(ipds_engine, logger):
    """Create intermediate prop_conditions table if needed."""
    table_name = f"msc.nvi_prop_conditions_{DATA_YEAR}"
    try:
        with ipds_engine.connect() as db:
            row = db.execute(text(f"SELECT COUNT(*) FROM {table_name};")).fetchone()
        logger.info(f"'{table_name}' already exists with {row.count} rows.")
    except Exception:
        logger.info(f"Creating '{table_name}'.")
        create_q = _load_sql("create_table_nvi_prop_conditions.sql")
        with ipds_engine.connect() as db:
            db.execute(create_q)


def _extract_from_queries(ipds_engine, logger):
    """Run SQL queries and combine into wide format."""
    combined_topics = []
    for filename in QUERY_FILES:
        logger.info(f"Running query '{filename}'.")
        table = pd.read_sql(_load_sql(filename), ipds_engine, params=PARAMS)
        if "year" not in table.columns:
            table["year"] = DATA_YEAR
        combined_topics.append(
            table.astype({"geography": "str"}).set_index(["geo_type", "geography", "year"])
        )
    return pd.concat(combined_topics, axis=1).reset_index()


@task("ipds", phase=1, description="Infrastructure, property, development & safety indicators")
def run(source: Engine, target: Engine) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    ipds_engine = get_engine("ipds")

    # Setup
    _setup_population_reference(ipds_engine, logger)
    _setup_location_types(ipds_engine, logger)
    _setup_intermediate_table(ipds_engine, logger)

    primary_indicators = pd.read_csv(CONF_DIR / "ipds" / "primary_indicator_ids.csv")
    total_rows = 0

    # Extract and transform from SQL queries
    query_wide = _extract_from_queries(ipds_engine, logger)
    query_wide["location_id"] = query_wide.apply(pin_location, axis=1)

    query_tall = (
        elongate(query_wide)
        .merge(primary_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .assign(value_type_id=1, survey_id=1)
    )

    total_rows += upsert_values(target, query_tall)

    # Context indicators
    context_indicators = pd.read_csv(CONF_DIR / "ipds" / "context_indicator_ids.csv")

    def _build_pct(indicator):
        def inner(df):
            num = df[f"count_{indicator}"]
            den = df[f"universe_{indicator}"]
            return (100 * num / den).round(0)
        return inner

    aggregations = {}
    for _, ind in context_indicators.iterrows():
        if ind["indicator_type"] == "count":
            continue
        elif ind["indicator_type"] in {"percentage", "rate"}:
            aggregations[f"{ind['indicator_type']}_{ind['indicator']}"] = _build_pct(ind["indicator"])

    context_wide = query_wide.assign(**aggregations)
    context_tall = (
        elongate(context_wide)
        .merge(context_indicators, on=["indicator", "year"], how="inner")
        .drop(["indicator", "geo_type", "geography", "indicator_type", "year", "filter_type_id"], axis=1, errors="ignore")
    )

    total_rows += upsert_context_values(target, context_tall)

    return TaskResult(task_name="ipds", rows_inserted=total_rows, success=True)
