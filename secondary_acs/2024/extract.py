from pathlib import Path
import pandas as pd
from d3census import (
    variable,
    Geography,
    create_geography,
    create_edition,
    build_profile,
)

import pandas as pd

WORKING_DIR = Path(__file__).resolve().parent
YEAR = 2023


# Define each indicator as a python function
@variable
def num_people_w_det_poverty_status(geo: Geography):
    return geo.C17002._001E


@variable
def num_people_below_200_fpl_acs(geo: Geography):
    """
    Universe: Population for whom poverty status is determined.
    """
    return sum(
        [  # type: ignore
            geo.C17002._002E,
            geo.C17002._003E,
            geo.C17002._004E,
            geo.C17002._005E,
            geo.C17002._006E,
            geo.C17002._007E,
        ]
    )


@variable
def num_people_above_200_fpl_acs(geo: Geography):
    """
    Universe: Population for whom poverty status is determined.
    """
    return geo.C17002._008E


@variable
def total_population_ge_25(geo: Geography):
    """
    Universe: Population 25 years and over.
    """
    return geo.B15003._001E


@variable
def population_ge_25_with_post_hs_diploma_ged(geo: Geography):
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
def population_ge_25_with_post_sec_degree(geo: Geography):
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
def total_households(geo: Geography):
    """
    Universe: Housing Units
    """
    return geo.B25106._001E


@variable
def owner_occupied_households(geo: Geography):
    """
    Universe: Housing Units
    """
    return geo.B25106._002E


@variable
def oo_hh_spending_lt_30(geo: Geography):
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
def renter_occupied_households(geo: Geography):
    """
    Universe: Housing Units
    """
    return geo.B25106._024E


@variable
def ro_hh_spending_lt_30(geo: Geography):
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
def population_over_20(geo: Geography):
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


@variable
def num_over_20_in_labor_force(geo: Geography):
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
def population_16_to_19(geo: Geography):
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


@variable
def num_16_to_19_in_labor_force(geo: Geography):
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
def total_population(geo: Geography):
    return geo.B01003._001E


@variable
def income_lt_10000(geo: Geography):
    return geo.B19001._002E


@variable
def income_10000_to_14999(geo: Geography):
    return geo.B19001._003E


@variable
def income_15000_to_19999(geo: Geography):
    return geo.B19001._004E


@variable
def income_20000_to_24999(geo: Geography):
    return geo.B19001._005E


@variable
def income_25000_to_29999(geo: Geography):
    return geo.B19001._006E


@variable
def income_30000_to_34999(geo: Geography):
    return geo.B19001._007E


@variable
def income_35000_to_39999(geo: Geography):
    return geo.B19001._008E


@variable
def income_40000_to_44999(geo: Geography):
    return geo.B19001._009E


@variable
def income_45000_to_49999(geo: Geography):
    return geo.B19001._010E


@variable
def income_50000_to_59999(geo: Geography):
    return geo.B19001._011E


@variable
def income_60000_to_74999(geo: Geography):
    return geo.B19001._012E


@variable
def income_75000_to_99999(geo: Geography):
    return geo.B19001._013E


@variable
def income_100000_to_124999(geo: Geography):
    return geo.B19001._014E


@variable
def income_125000_to_149999(geo: Geography):
    return geo.B19001._015E


@variable
def income_150000_to_199999(geo: Geography):
    return geo.B19001._016E


@variable
def income_ge_200000(geo: Geography):
    return geo.B19001._017E


@variable
def num_white_alone(geo: Geography):
    return geo.B03002._003E


@variable
def num_black_or_african_american_alone(geo: Geography):
    return geo.B03002._004E


@variable
def num_american_indian_and_alaska_native_alone(geo: Geography):
    return geo.B03002._005E


@variable
def num_asian_alone(geo: Geography):
    return geo.B03002._006E


@variable
def num_native_hawaiian_and_other_pacific_islander_alone(geo: Geography):
    return geo.B03002._007E


@variable
def num_some_other_race_alone(geo: Geography):
    return geo.B03002._008E


@variable
def num_two_or_more_races(geo: Geography):
    return geo.B03002._009E


@variable
def num_hispanic_or_latino(geo: Geography):
    return geo.B03002._012E


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


# Tract-level:
def pull_tract_level(logger):
    profile_one = build_profile(
        variables=[
            total_population_ge_25,
            population_ge_25_with_post_hs_diploma_ged,
            population_ge_25_with_post_sec_degree,
            num_people_w_det_poverty_status,
            num_people_below_200_fpl_acs,
            num_people_above_200_fpl_acs,
            total_households,
            owner_occupied_households,
            oo_hh_spending_lt_30,
            renter_occupied_households,
            ro_hh_spending_lt_30,
        ],
        geographies=[
            # Geography(state="26", county="163", county_subdivision="22000"),
            create_geography(state="26", county="163", tract="*"),
        ],
        edition=create_edition("acs5", YEAR),
    )

    profile_two = build_profile(
        variables=[
            population_over_20,
            num_over_20_in_labor_force,
        ],
        geographies=[
            # Geography(state="26", county="163", county_subdivision="22000"),
            create_geography(state="26", county="163", tract="*"),
        ],
        edition=create_edition("acs5", YEAR),
    )

    profile_three = build_profile(
        variables=[
            population_16_to_19,
            num_16_to_19_in_labor_force,
            total_population,
            num_households_with_children,
        ],
        geographies=[
            # Geography(state="26", county="163", county_subdivision="22000"),
            create_geography(state="26", county="163", tract="*"),
        ],
        edition=create_edition("acs5", YEAR),
    )

    # Take apart hierfindal for roll-up

    income_profile = build_profile(
        variables=[
            income_lt_10000,
            income_10000_to_14999,
            income_20000_to_24999,
            income_25000_to_29999,
            income_30000_to_34999,
            income_35000_to_39999,
            income_40000_to_44999,
            income_45000_to_49999,
            income_50000_to_59999,
            income_60000_to_74999,
            income_75000_to_99999,
            income_100000_to_124999,
            income_125000_to_149999,
            income_150000_to_199999,
            income_ge_200000,
        ],
        geographies=[
            create_geography(state="26", county="163", tract="*"),
        ],
        edition=create_edition("acs5", YEAR),
    )

    race_eth_profile = build_profile(
        variables=[
            num_white_alone,
            num_black_or_african_american_alone,
            num_american_indian_and_alaska_native_alone,
            num_asian_alone,
            num_native_hawaiian_and_other_pacific_islander_alone,
            num_some_other_race_alone,
            num_two_or_more_races,
            num_hispanic_or_latino,
        ],
        geographies=[
            create_geography(state="26", county="163", tract="*"),
        ],
        edition=create_edition("acs5", YEAR),
    )

    chilrden_pov_profile = build_profile(
        variables=[
            num_children_below_pov,
            num_children_above_pov,
        ],
        geographies=[
            create_geography(state="26", county="163", tract="*"),
        ],
        edition=create_edition("acs5", YEAR),
    )

    profile = pd.concat(
        [
            profile_one.set_index(["geoid", "name"]),
            profile_two.set_index(["geoid", "name"]),
            profile_three.set_index(["geoid", "name"]),
            income_profile.set_index(["geoid", "name"]),
            race_eth_profile.set_index(["geoid", "name"]),
            chilrden_pov_profile.set_index(["geoid", "name"]),
        ],
        axis=1,
    )

    logger.info(f"Number of tracks in dataset (should be ~630): {len(profile)}") 
    profile.to_parquet(WORKING_DIR / "output" / f"nvi_tracts_{YEAR}.parquet.gzip")


# City-wide:
def pull_city_wide(logger):
    profile_one = build_profile(
        variables=[
            total_population_ge_25,
            population_ge_25_with_post_hs_diploma_ged,
            population_ge_25_with_post_sec_degree,
            num_people_w_det_poverty_status,
            num_people_below_200_fpl_acs,
            num_people_above_200_fpl_acs,
            total_households,
            owner_occupied_households,
            oo_hh_spending_lt_30,
            renter_occupied_households,
            ro_hh_spending_lt_30,
        ],
        geographies=[
            create_geography(
                state="26", county="163", county_subdivision="22000"
            ),
        ],
        edition=create_edition("acs5", YEAR),
    )

    profile_two = build_profile(
        variables=[
            population_over_20,
            num_over_20_in_labor_force,
        ],
        geographies=[
            create_geography(
                state="26", county="163", county_subdivision="22000"
            ),
        ],
        edition=create_edition("acs5", YEAR),
    )

    profile_three = build_profile(
        variables=[
            population_16_to_19,
            num_16_to_19_in_labor_force,
            total_population,
            num_households_with_children,
        ],
        geographies=[
            create_geography(
                state="26", county="163", county_subdivision="22000"
            ),
        ],
        edition=create_edition("acs5", YEAR),
    )

    # Take apart hierfindal for roll-up

    income_profile = build_profile(
        variables=[
            income_lt_10000,
            income_10000_to_14999,
            income_20000_to_24999,
            income_25000_to_29999,
            income_30000_to_34999,
            income_35000_to_39999,
            income_40000_to_44999,
            income_45000_to_49999,
            income_50000_to_59999,
            income_60000_to_74999,
            income_75000_to_99999,
            income_100000_to_124999,
            income_125000_to_149999,
            income_150000_to_199999,
            income_ge_200000,
        ],
        geographies=[
            create_geography(
                state="26", county="163", county_subdivision="22000"
            ),
        ],
        edition=create_edition("acs5", YEAR),
    )

    race_eth_profile = build_profile(
        variables=[
            num_white_alone,
            num_black_or_african_american_alone,
            num_american_indian_and_alaska_native_alone,
            num_asian_alone,
            num_native_hawaiian_and_other_pacific_islander_alone,
            num_some_other_race_alone,
            num_two_or_more_races,
            num_hispanic_or_latino,
        ],
        geographies=[
            create_geography(
                state="26", county="163", county_subdivision="22000"
            ),
        ],
        edition=create_edition("acs5", YEAR),
    )

    chilrden_pov_profile = build_profile(
        variables=[
            num_children_below_pov,
            num_children_above_pov,
        ],
        geographies=[
            create_geography(
                state="26", county="163", county_subdivision="22000"
            ),
        ],
        edition=create_edition("acs5", YEAR),
    )

    profile = pd.concat(
        [
            profile_one.set_index(["geoid", "name"]),
            profile_two.set_index(["geoid", "name"]),
            profile_three.set_index(["geoid", "name"]),
            income_profile.set_index(["geoid", "name"]),
            race_eth_profile.set_index(["geoid", "name"]),
            chilrden_pov_profile.set_index(["geoid", "name"]),
        ],
        axis=1,
    )

    logger.info(f"Number of cities in dataset (should be 1): {len(profile)}")
    profile.to_parquet(WORKING_DIR / "output" / f"nvi_citywide_{YEAR}.parquet.gzip")


def extract(logger):
    """
    This data doesn't change frequently (if ever), so 'extract' checks 
    to see if there are files from previous extracts available to avoid
    hitting the API unnecessairily.
    """

    if (WORKING_DIR / "input" / f"nvi_tracts_{YEAR}.parquet.gzip").exists():
        logger.info(
            "Tract-level already pulled--remove file from 'output' to pull again."
        )
    else:
        logger.info("Pulling the tract level ACS data")
        pull_tract_level(logger)

    if (WORKING_DIR / "input" / f"nvi_citywide_{YEAR}.parquet.gzip").exists():
        logger.info(
            "City-wide already pulled-- remove file from 'output' to pull again."
        )
    else:
        logger.info("Pulling the city-wide ACS data")
        pull_city_wide(logger)

