import configparser
from pathlib import Path
import geopandas as gpd

from nvi_etl import make_engine_for


WORKING_DIR = Path(__file__).resolve().parent

config = configparser.ConfigParser()
config.read(WORKING_DIR / "conf" / ".conf")


def transform_holc():
    filename = config["source_files"]["holc"]

    holc = gpd.read_file(filename).to_crs(2898)
    holc_detroit = holc[holc["city"] == "Detroit"]

    nvi_engine = make_engine_for("data")

    holc_detroit.to_postgis("holc_maps", nvi_engine, schema="nvi", index=False)


if __name__ == "__main__":
    transform_holc()