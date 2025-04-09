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


WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023
COMPARISON_YEARS = [2013, 2019]

# 'VALUE' INDICATORS

# 1. Above 200% Federal Poverty Line


@variable
def count_above_200_fpl(geo: Geography):
    """
    Total 200% and over
    """
    return geo.C17002._008E


@variable
def universe_above_200_fpl(geo: Geography):
    """
    People for whom poverty status is determined.
    """
    return geo.C17002._001E


# 'percentage' calculated on transform step


# 2. HS Diploma


@variable
def count_hs_diploma(geo: Geography):
    """
    Universe: Population 25 years and over.
    """
    return sum(
        [  # type: ignore
            geo.B15003._017E,
            geo.B15003._018E,
            geo.B15003._019E,
            geo.B15003._020E,
            geo.B15003._021E,
            geo.B15003._022E,
            geo.B15003._023E,
            geo.B15003._024E,  # Professional school degree (JD? others)
            geo.B15003._025E,
        ]
    )


@variable
def universe_hs_diploma(geo: Geography):
    """
    Universe: Population 25 years and over.
    """
    return geo.B15003._001E


# 3. Post-secondary degree


@variable
def count_postsecondary(geo: Geography):
    """
    Universe: Population 25 years and over.
    """
    return sum(
        [  # type: ignore
            geo.B15003._021E,
            geo.B15003._022E,
            geo.B15003._023E,
            geo.B15003._024E,  # Professional school degree (JD? others)
            geo.B15003._025E,
        ]
    )


@variable
def universe_postsecondary(geo: Geography):
    """
    Universe: Population 25 years and over.
    """
    return geo.B15003._001E


# 4. Owner-occupied not cost burdened


@variable
def count_oo_no_cost_burden(geo: Geography):
    """
    Universe: Owner-Occupied Housing Units

    Summing across all income brackets.
    """
    return sum(
        [  # type: ignore
            geo.B25106._004E,  # Owner Occupied
            geo.B25106._005E,
            geo.B25106._008E,
            geo.B25106._009E,
            geo.B25106._012E,
            geo.B25106._013E,
            geo.B25106._016E,
            geo.B25106._017E,
            geo.B25106._020E,
            geo.B25106._021E,
        ]
    )


@variable
def universe_oo_no_cost_burden(geo: Geography):
    return geo.B25003._002E


# 5. Renter-occupied not cost burdened


@variable
def count_ro_no_cost_burden(geo: Geography):
    """
    Universe: Renter-Occupied Housing Units

    Summing across all income brackets.
    """

    return sum(
        [  # type: ignore
            geo.B25106._026E,
            geo.B25106._027E,
            geo.B25106._030E,
            geo.B25106._031E,
            geo.B25106._034E,
            geo.B25106._035E,
            geo.B25106._038E,
            geo.B25106._039E,
            geo.B25106._042E,
            geo.B25106._043E,
        ]
    )


@variable
def universe_ro_no_cost_burden(geo: Geography):
    """
    Universe: Housing Units
    """
    return geo.B25106._024E


# 6. Over 20 seeking employment


@variable
def count_over_20_seeking_emp(geo: Geography):
    """
    Table Universe: Population 16 years and over
    Indicator Universe: population 20 years and older
    """

    return sum(
        [  # type: ignore
            geo.B23001._011E,  # Male
            geo.B23001._018E,
            geo.B23001._025E,
            geo.B23001._032E,
            geo.B23001._039E,
            geo.B23001._046E,
            geo.B23001._053E,
            geo.B23001._060E,
            geo.B23001._067E,
            geo.B23001._074E,
            geo.B23001._079E,
            geo.B23001._084E,
            geo.B23001._097E,  # Female
            geo.B23001._104E,
            geo.B23001._111E,
            geo.B23001._118E,
            geo.B23001._125E,
            geo.B23001._132E,
            geo.B23001._139E,
            geo.B23001._146E,
            geo.B23001._153E,
            geo.B23001._160E,
            geo.B23001._165E,
            geo.B23001._170E,
        ]
    )


@variable
def universe_over_20_seeking_emp(geo: Geography):
    """
    Table Universe: Population 16 years and over
    Indicator Universe: population 20 years and older
    """

    return sum(
        [  # type: ignore
            geo.B23001._010E,  # Male
            geo.B23001._017E,
            geo.B23001._024E,
            geo.B23001._031E,
            geo.B23001._038E,
            geo.B23001._045E,
            geo.B23001._052E,
            geo.B23001._059E,
            geo.B23001._066E,
            geo.B23001._073E,
            geo.B23001._078E,
            geo.B23001._083E,
            geo.B23001._096E,  # Female
            geo.B23001._103E,
            geo.B23001._110E,
            geo.B23001._117E,
            geo.B23001._124E,
            geo.B23001._131E,
            geo.B23001._138E,
            geo.B23001._145E,
            geo.B23001._152E,
            geo.B23001._159E,
            geo.B23001._164E,
            geo.B23001._169E,
        ]
    )


# 7. 16-19 seeking employment


@variable
def count_btwn_16_19_seeking_emp(geo: Geography):
    """
    Table Universe: Population 16 years and over
    Indicator Universe: population 20 years and older
    """

    return sum(
        [  # type: ignore
            geo.B23001._004E,
            geo.B23001._090E,
        ]
    )


@variable
def universe_btwn_16_19_seeking_emp(geo: Geography):
    """
    Table Universe: Population 16 years and over
    Indicator Universe: population 20 years and older
    """

    return sum(
        [  # type: ignore
            geo.B23001._003E,
            geo.B23001._089E,
        ]
    )


# 'CONTEXT' Indicators

# 1. Population


@variable
def count_population(geo: Geography):
    return geo.B01003._001E


# 2. Population under 18


@variable
def count_population_under_18(geo: Geography):
    return sum(
        [
            geo.B01001._003E,
            geo.B01001._004E,
            geo.B01001._005E,
            geo.B01001._006E,
            geo.B01001._027E,
            geo.B01001._028E,
            geo.B01001._029E,
            geo.B01001._030E,
        ]
    )


# 3. Population over 65


@variable
def count_population_over_65(geo: Geography):
    return sum(
        [
            geo.B01001._020E,
            geo.B01001._021E,
            geo.B01001._022E,
            geo.B01001._023E,
            geo.B01001._024E,
            geo.B01001._025E,
            geo.B01001._044E,
            geo.B01001._045E,
            geo.B01001._046E,
            geo.B01001._047E,
            geo.B01001._048E,
            geo.B01001._049E,
        ]
    )


# 4. Home ownership rate


@variable
def count_home_ownership_rate(geo: Geography):
    return geo.B25003._002E


@variable
def universe_home_ownership_rate(geo: Geography):
    return geo.B25003._001E


# 5. Occupancy


@variable
def count_occupancy(geo: Geography):
    return geo.B25003._001E


@variable
def universe_occupancy(geo: Geography):
    return geo.B25002._001E


# 6. Married couples


@variable
def count_married_couples(geo: Geography):
    return geo.B11012._002E


@variable
def universe_married_couples(geo: Geography):
    return geo.B11012._001E


# 7. Female householder, no spouse present


@variable
def count_female_householder(geo: Geography):
    return geo.B11012._008E


@variable
def universe_female_householder(geo: Geography):
    return geo.B11012._001E


# 8. Male householder, no spose present


@variable
def count_male_householder(geo: Geography):
    return geo.B11012._013E


@variable
def universe_male_householder(geo: Geography):
    return geo.B11012._001E


# 9. Non-family household -- Skipping for now!
# @variable
# def non_family_hhs(geo: Geography):
# return geo.B11012.


# 10 - who knows -- Ages

# Grabbing these for estimating median housing value

@variable
def hh_children(geo: Geography):
    """
    Universe: Households
    """
    return geo.B11005._002E


@variable
def num_children_below_pov(geo: Geography):
    return sum(
        [  # type: ignore
            geo.B17001._004E,  # Male
            geo.B17001._005E,
            geo.B17001._006E,
            geo.B17001._007E,
            geo.B17001._008E,
            geo.B17001._009E,
            geo.B17001._018E,  # Female
            geo.B17001._019E,
            geo.B17001._020E,
            geo.B17001._021E,
            geo.B17001._022E,
            geo.B17001._023E,
        ]
    )


@variable
def num_children_above_pov(geo: Geography):
    return sum(
        [  # type: ignore
            geo.B17001._033E,  # Male
            geo.B17001._034E,
            geo.B17001._035E,
            geo.B17001._036E,
            geo.B17001._037E,
            geo.B17001._038E,
            geo.B17001._047E,  # Female
            geo.B17001._048E,
            geo.B17001._049E,
            geo.B17001._050E,
            geo.B17001._051E,
            geo.B17001._052E,
        ]
    )


@variable
def num_households_with_children(geo: Geography):
    return geo.B11005._002E


# The d3variable can only pull 50 variables at once (a fix to come) so
# we have to pull in batches.


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
            count_above_200_fpl,
            universe_above_200_fpl,
            count_hs_diploma,
            universe_hs_diploma,
            count_postsecondary,
            universe_postsecondary,
            count_oo_no_cost_burden,
            universe_oo_no_cost_burden,
            count_ro_no_cost_burden,
            universe_ro_no_cost_burden,
            count_over_20_seeking_emp,
            universe_over_20_seeking_emp,
            count_btwn_16_19_seeking_emp,
            universe_btwn_16_19_seeking_emp,
            count_population,
            count_population_under_18,
            count_population_over_65,
            # count_pop_per_sq_mi,
            count_home_ownership_rate,
            count_occupancy,
            count_married_couples,
            count_male_householder,
            count_female_householder,
            # count_non_family,
            # count_child_below_poverty,
            # count_median_housing_value,
            # universe_pop_per_sq_mi,
            universe_home_ownership_rate,
            universe_occupancy,
            universe_married_couples,
            universe_male_householder,
            universe_female_householder,
            # universe_non_family,
            # universe_child_below_poverty,
            # universe_median_housing_value,
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
                count_above_200_fpl,
                universe_above_200_fpl,
                count_hs_diploma,
                universe_hs_diploma,
                count_postsecondary,
                universe_postsecondary,
                count_oo_no_cost_burden,
                universe_oo_no_cost_burden,
                count_ro_no_cost_burden,
                universe_ro_no_cost_burden,
                count_over_20_seeking_emp,
                universe_over_20_seeking_emp,
                count_btwn_16_19_seeking_emp,
                universe_btwn_16_19_seeking_emp,
                count_population,
                count_population_under_18,
                count_population_over_65,
                # count_pop_per_sq_mi,
                count_home_ownership_rate,
                count_occupancy,
                count_married_couples,
                count_male_householder,
                count_female_householder,
                # count_non_family,
                # count_child_below_poverty,
                # count_median_housing_value,
                # universe_pop_per_sq_mi,
                universe_home_ownership_rate,
                universe_occupancy,
                universe_married_couples,
                universe_male_householder,
                universe_female_householder,
                # universe_non_family,
                # universe_child_below_poverty,
                # universe_median_housing_value,
            ],
            edition,
        ).assign(year=year)
        comparisons.append(profile)

    pd.concat(comparisons).to_parquet(
        WORKING_DIR / "input" / f"nvi_2024_acs.parquet.gzip"
    )
