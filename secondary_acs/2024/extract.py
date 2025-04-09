from pathlib import Path
import pandas as pd
from d3census import (
    variable,
    Geography,
    create_geography,
    create_edition,
    build_profile,
)
from age_distribution import AGE_DISTRIBUTION_VARIABLES
from income_distribution import INCOME_DISTRIBUTION_VARIABLES
from race_ethnicity import RACE_ETHNICITY_VARIABLES
from home_value_distribution import HOME_VALUE_DISTRIBUTION_VARIABLES
from other_indicators import OTHER_INDICATORS


WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023
COMPARISON_YEARS = [2013, 2018]

# 'VALUE' INDICATORS

# 1. Above 200% Federal Poverty Line


def extract(logger):
    """
    This data doesn't change frequently (if ever), so 'extract' checks
    to see if there are files from previous extracts available to avoid
    hitting the API unnecessairily.
    """

    DETROIT = create_geography(
        state="26", county="163", county_subdivision="22000"
    )
    WAYNE_TRACTS = create_geography(state="26", county="163", tract="*")

    if (WORKING_DIR / "input" / f"nvi_2024_acs.parquet.gzip").exists():
        logger.info(
            "Tract-level already pulled--remove file from 'output' to pull again."
        )
        return

    logger.info(f"Pulling all ACS data for {YEAR}")

    edition = create_edition("acs5", YEAR)
    acs_present = build_profile(
        [DETROIT, WAYNE_TRACTS],
        [
            *AGE_DISTRIBUTION_VARIABLES,
            *RACE_ETHNICITY_VARIABLES,
            *INCOME_DISTRIBUTION_VARIABLES,
            *HOME_VALUE_DISTRIBUTION_VARIABLES,
            *OTHER_INDICATORS,
        ],
        edition,
    ).assign(
        year=YEAR,
    )

    comparisons = [acs_present]
    for year in COMPARISON_YEARS:
        edition = create_edition("acs5", year)
        profile = build_profile(
            [DETROIT, WAYNE_TRACTS],
            [
                *OTHER_INDICATORS
            ],
            edition,
        ).assign(year=year)
        comparisons.append(profile)

    pd.concat(comparisons).to_parquet(
        WORKING_DIR / "input" / f"nvi_2024_acs.parquet.gzip"
    )
