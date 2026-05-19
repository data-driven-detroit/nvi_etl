"""Geographic boundary loading tasks.

Loads neighborhood zones, council districts, city boundary, tract crosswalk,
CDO zones, and HOLC maps into the PostGIS database.
"""

import json
import configparser
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import BASE_DIR
from nvi_etl.registry import task, TaskResult
from nvi_etl.utilities import calculate_square_miles

# All geography configs live under aux_geography/2024/
GEO_BASE = BASE_DIR / "aux_geography" / "2024"


def _read_conf(geo_dir: Path) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(geo_dir / "conf" / ".conf")
    return config


def _read_field_ref(geo_dir: Path) -> dict:
    return json.loads((geo_dir / "conf" / "field_reference.json").read_text())


@task("geography_neighborhood_zones", phase=1, description="Load NVI neighborhood zones")
def run_neighborhood_zones(source: Engine, target: Engine) -> TaskResult:
    geo_dir = GEO_BASE / "neighborhood_zones"
    config = _read_conf(geo_dir)
    field_ref = _read_field_ref(geo_dir)

    nzs = (
        gpd.read_file(config["source_files"]["neighborhood_zones"])
        .to_crs(crs="EPSG:2898")
        .rename(columns=field_ref["rename"])
        .assign(
            start_date=date.fromisoformat("2026-01-01"),
            end_date=date.fromisoformat("2036-12-31"),
            square_miles=calculate_square_miles,
        )
    )[field_ref["order"]]

    nzs.to_postgis("neighborhood_zones", target, schema="nvi", if_exists="append", index=False)

    return TaskResult(task_name="geography_neighborhood_zones", rows_inserted=len(nzs), success=True)


@task("geography_council_districts", phase=1, description="Load Detroit council districts")
def run_council_districts(source: Engine, target: Engine) -> TaskResult:
    geo_dir = GEO_BASE / "council_districts"
    config = _read_conf(geo_dir)
    field_ref = _read_field_ref(geo_dir)

    cds = (
        gpd.read_file(config["source_files"]["council_districts"])
        .rename(columns=field_ref["renames"])
        .to_crs(crs="EPSG:2898")
        .assign(
            start_date=date.fromisoformat("2026-01-01"),
            end_date=date.fromisoformat("2036-12-31"),
            square_miles=calculate_square_miles,
        )
    )[field_ref["column_order"]]

    cds.to_postgis("detroit_council_districts", target, schema="nvi", if_exists="append", index=False)

    return TaskResult(task_name="geography_council_districts", rows_inserted=len(cds), success=True)


@task("geography_city_boundary", phase=1, description="Load Detroit city boundary")
def run_city_boundary(source: Engine, target: Engine) -> TaskResult:
    geo_dir = GEO_BASE / "city_boundary"
    config = _read_conf(geo_dir)
    field_ref = _read_field_ref(geo_dir)

    boundary = (
        gpd.read_file(config["source_files"]["city_boundary"])
        .to_crs(crs="EPSG:2898")
        .rename(columns=field_ref["renames"])
        .assign(
            geoid="06000US2616322000",
            start_date=date.fromisoformat("2026-01-01"),
            end_date=date.fromisoformat("2099-12-31"),
            square_miles=calculate_square_miles,
        )
    )[field_ref["column_order"]]

    boundary.to_postgis("city_boundary", target, schema="nvi", if_exists="append", index=False)

    return TaskResult(task_name="geography_city_boundary", rows_inserted=len(boundary), success=True)


@task("geography_tract_crosswalk", phase=1, description="Load tract-to-zone crosswalk")
def run_tract_crosswalk(source: Engine, target: Engine) -> TaskResult:
    geo_dir = GEO_BASE / "tract_crosswalk"
    config = _read_conf(geo_dir)
    field_ref = _read_field_ref(geo_dir)

    nzs = (
        gpd.read_file(config["source_files"]["district_zone_tract_crosswalk"])
        .set_crs(crs="EPSG:2898")
        .rename(columns=field_ref["renames"])
        .assign(
            tract_geoid=lambda df: "14000US" + df["__tract_geoid"],
            tract_start_date=date.fromisoformat("2020-01-01"),
            tract_end_date=date.fromisoformat("2029-12-31"),
            zone_start_date=date.fromisoformat("2026-01-01"),
            zone_end_date=date.fromisoformat("2036-12-31"),
        )
    )[field_ref["column_order"]]

    nzs.to_sql("tracts_to_nvi_crosswalk", target, schema="nvi", if_exists="replace", index=False)

    return TaskResult(task_name="geography_tract_crosswalk", rows_inserted=len(nzs), success=True)


@task("geography_cdo_zones", phase=1, description="Load CDO service area boundaries")
def run_cdo_zones(source: Engine, target: Engine) -> TaskResult:
    geo_dir = GEO_BASE / "cdo_zones"
    config = _read_conf(geo_dir)
    field_ref = _read_field_ref(geo_dir)

    file = (
        gpd.read_file(config["source_files"]["cdo_boundaries"])
        .rename(columns=field_ref["renames"])
        [field_ref["column_order"]]
        .assign(
            start_date=pd.to_datetime("2025-01-01"),
            end_date=pd.to_datetime("2199-01-01"),
        )
    )

    file.to_postgis("cdo_boundaries", target, schema="nvi", if_exists="append", index=False)

    return TaskResult(task_name="geography_cdo_zones", rows_inserted=len(file), success=True)


@task("geography_holc", phase=1, description="Load HOLC redlining maps")
def run_holc(source: Engine, target: Engine) -> TaskResult:
    geo_dir = GEO_BASE / "holc"
    config = _read_conf(geo_dir)

    holc = gpd.read_file(config["source_files"]["holc"]).to_crs(2898)
    holc_detroit = holc[holc["city"] == "Detroit"]

    holc_detroit.to_postgis("holc_maps", target, schema="nvi", index=False)

    return TaskResult(task_name="geography_holc", rows_inserted=len(holc_detroit), success=True)
