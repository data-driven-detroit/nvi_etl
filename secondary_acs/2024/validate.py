from pathlib import Path
import pandas as pd
from nvi_etl.schema import NVIValueTable


WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023


def validate_acs_to_nvi_schema(logger):
    """
    Make sure the ACS dataset aligns with the output schema
    """
    logger.info("Validating ACS indicators for NVI schema.")

    citywide = pd.read_csv(WORKING_DIR / "output" / f"nvi_citywide_{YEAR}.csv")

    # Validate file
    NVIValueTable.validate(citywide)

    districts = pd.read_csv(WORKING_DIR / "output" / f"nvi_districts_{YEAR}.csv")
    
    NVIValueTable.validate(districts)

    # If this breaks, it throws a helpful error
    logger.info("SUCCESS!")

