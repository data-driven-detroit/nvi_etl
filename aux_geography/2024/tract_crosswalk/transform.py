import json
import configparser
from pathlib import Path
from datetime import date
import geopandas as gpd


WORKING_DIR = Path(__file__).resolve().parent


config = configparser.ConfigParser()
config.read(WORKING_DIR / "conf" / ".conf")


def transform_2020_tracts_to_2026_nvi_cw(logger):
    logger.info("Transforming tracts to council_districts / neighborhood zones crosswalk.")
    
    field_reference = json.loads(
        (WORKING_DIR / "conf" / "field_reference.json").read_text()
    )

    nzs = (
        gpd.read_file(config["source_files"]["district_zone_tract_crosswalk"])
        .set_crs(crs="EPSG:2898")
        .rename(columns=field_reference["renames"])
        .assign(
            tract_geoid = lambda df: "14000US" + df["__tract_geoid"],
            tract_start_date=date.fromisoformat("2020-01-01"),
            tract_end_date=date.fromisoformat("2029-12-31"),
            zone_start_date=date.fromisoformat("2026-01-01"),
            zone_end_date=date.fromisoformat("2036-12-31"),
        )
    )[field_reference["column_order"]]

    nzs.to_csv(
        WORKING_DIR / "output" / "tracts_2020_to_zones_2026_crosswalk.csv", index=False
    )