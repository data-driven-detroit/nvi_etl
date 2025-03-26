import json
from nvi_etl import working_dir, liquefy
import pandas as pd


WORKING_DIR = working_dir(__file__)
location_map = json.loads((WORKING_DIR / "conf" / "location_map.json").read_text())


def pin_location_id(row):
    return location_map[row["geo_type"]][row["geography"]]



def transform_mischooldata(logger):
    logger.warning("Transforming mischooldata datasets.")

    file = (
        pd.read_csv(WORKING_DIR / "output" / "g3_ela_2023.csv")
        .assign(
            location_id=lambda df: df.apply(pin_location_id, axis=1)
        )
    )

    file.to_csv(WORKING_DIR / "output" / "g3_ela_2023_tmp.csv", index=False)

    melted = liquefy(file)

    melted.to_csv(WORKING_DIR / "output" / "g3_ela_2023_melted.csv", index=False)

