from pathlib import Path
import geopandas as gpd
import pandas as pd

from nvi_etl import make_engine_for


WORKING_DIR = Path(__file__).resolve().parent

engine = make_engine_for("data")

def load_cdo_boundaries():
    file = (
        gpd.read_file(WORKING_DIR / "output" / "cdo_boundaries_2026.geojson")
        .assign(
            start_date=pd.to_datetime("2018-01-01"), 
            end_date=pd.to_datetime("2199-01-01"),
        )
    )

    file.to_postgis(
        "cdo_boundaries", engine, schema="nvi", if_exists="append"
    )


if __name__ == "__main__":
    load_cdo_boundaries()
