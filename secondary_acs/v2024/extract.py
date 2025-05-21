from pathlib import Path
import pandas as pd
from d3census import create_geography, create_edition, build_profile

from variables.age_distribution import AGE_DISTRIBUTION_VARIABLES
from variables.income_distribution import INCOME_DISTRIBUTION_VARIABLES
from variables.race_ethnicity import RACE_ETHNICITY_VARIABLES
from variables.home_value_distribution import HOME_VALUE_DISTRIBUTION_VARIABLES
from variables.other_indicators import OTHER_INDICATORS
from variables.over_time import OVERTIME_INDICATORS
from variables.gross_rent_distribution import GROSS_RENT_DISTRIBUTION


WORKING_DIR = Path(__file__).resolve().parent
YEARS = [2013, 2018, 2023]

# 'VALUE' INDICATORS

# 1. Above 200% Federal Poverty Line


def extract(logger):
    """
    This data doesn't change frequently (if ever), so 'extract' checks
    to see if there are files from previous extracts available to avoid
    hitting the API unnecessairily.

    KEEP IN MIND: none of these function in the 'extract' step should
    be turned into percentages or combined in any other non-additive way.
    Those aggregations will happen after the roll up to neighborhood 
    zones or council districts.
    """

    DETROIT = create_geography(
        state="26", county="163", county_subdivision="22000"
    )
    WAYNE_TRACTS = create_geography(state="26", county="163", tract="*")

    if (WORKING_DIR / "input" / f"nvi_2024_acs.csv").exists():
        logger.info(
            "Tract-level already pulled--remove file from 'output' to pull again."
        )
        return


    # Some indicators we're only pulling for one year
    logger.info(f"Pulling all ACS data for {YEARS[-1]}")

    edition = create_edition("acs5", YEARS[-1])
    acs_present = build_profile(
        [DETROIT, WAYNE_TRACTS],
        [
            *AGE_DISTRIBUTION_VARIABLES,
            *GROSS_RENT_DISTRIBUTION,
            *RACE_ETHNICITY_VARIABLES,
            *INCOME_DISTRIBUTION_VARIABLES,
            *HOME_VALUE_DISTRIBUTION_VARIABLES,
            *OTHER_INDICATORS
        ],
        edition,
    ).assign(year=YEARS[-1])

    # Other indicators are pulled over time
    comparisons = [acs_present]
    for year in YEARS:
        logger.info(f"Pulling overtime data for {year}")
        edition = create_edition("acs5", year)
        profile = build_profile(
            [DETROIT, WAYNE_TRACTS],
            OVERTIME_INDICATORS,
            edition,
        ).assign(year=year)
        comparisons.append(profile)

    # Save everything compiled into one document
    pd.concat(comparisons).to_csv(
        WORKING_DIR / "input" / f"nvi_2024_acs.csv", index=False
    )


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    extract(logger)