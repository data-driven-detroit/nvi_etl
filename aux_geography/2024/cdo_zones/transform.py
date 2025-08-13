import json
import configparser
from datetime import date
from pathlib import Path
import geopandas as gpd

# from nvi_etl.utilities import calculate_square_miles

WORKING_DIR = Path(__file__).resolve().parent


def transform_cdo_boundaries():

    config = configparser.ConfigParser()
    config.read(WORKING_DIR / "conf" / ".conf")


    field_reference = json.loads(
        (
            WORKING_DIR / 
            "conf" / 
            "field_reference.json"
        ).read_text()
    )

    file = (
        gpd.read_file(config["source_files"]["cdo_boundaries"])
        .rename(columns=field_reference["renames"])
        [field_reference["column_order"]]
    )

    print(file.columns)

    file.to_file(WORKING_DIR / "output" / "cdo_boundaries_2026.geojson")


if __name__ == "__main__":
    transform_cdo_boundaries()
