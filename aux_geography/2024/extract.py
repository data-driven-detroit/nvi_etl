from pathlib import Path
import pandas as pd
from sqlalchemy import text

from nvi_etl import db_engine


WORKING_DIR = Path(__file__).resolve().parent


def extract_council_districts(logger):
    logger.info("No extraction required.")


def extract_neighborhood_zones(logger):
    logger.info("No extraction required.")

def extract_2020_tracts_to_2026_nvi_cw(logger):
    logger.info("No extraction required.")