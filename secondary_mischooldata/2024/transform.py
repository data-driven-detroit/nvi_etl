from nvi_etl import working_dir, liquefy
import pandas as pd


WORKING_DIR = working_dir(__file__)


def transform_mischooldata(logger):
    logger.warning("Transforming mischooldata datasets.")

    file = pd.read_csv(WORKING_DIR / "output" / "g3_ela_2023.csv")

    melted = liquefy(file)

    melted.to_csv(WORKING_DIR / "output" / "g3_ela_2023_melted.csv")

