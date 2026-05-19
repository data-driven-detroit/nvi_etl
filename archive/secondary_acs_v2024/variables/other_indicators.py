from d3census import variable, Geography

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
def count_married_couples(geo: Geography):
    return geo.B11001._003E


@variable
def universe_married_couples(geo: Geography):
    return geo.B11001._001E


# 7. Female householder, no spouse present


@variable
def count_female_householder(geo: Geography):
    return geo.B11001._006E


@variable
def universe_female_householder(geo: Geography):
    return geo.B11001._001E


# 8. Male householder, no spose present

@variable
def count_male_householder(geo: Geography):
    return geo.B11001._005E


@variable
def universe_male_householder(geo: Geography):
    return geo.B11001._001E


# 9. Non-family household -- Skipping for now!
@variable
def count_non_family(geo: Geography):
    return geo.B11001._007E

@variable
def universe_non_family(geo: Geography):
    return geo.B11001._001E

# 10 - who knows -- Ages

@variable
def count_under_eighteen_below_poverty(geo: Geography):
    return sum([
        geo.B17020._003E,
        geo.B17020._004E,
        geo.B17020._005E,
    ])


@variable
def universe_under_eighteen_below_poverty(geo: Geography):
    return sum([
        geo.B17020._003E,
        geo.B17020._004E,
        geo.B17020._005E,
        geo.B17020._011E,
        geo.B17020._012E,
        geo.B17020._013E,
    ])


@variable
def count_hh_w_children(geo: Geography):
    return geo.B11005._002E


@variable
def universe_hh_w_children(geo: Geography):
    return geo.B11005._001E


@variable
def count_children_five_nine_enrolled(geo: Geography):
    return sum([
        geo.B14003._005E,
        geo.B14003._014E,
        geo.B14003._033E,
        geo.B14003._042E,
    ])


# The d3variable can only pull 50 variables at once (a fix to come) so
# we have to pull in batches.

OTHER_INDICATORS = [
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
    count_married_couples,
    universe_married_couples,
    count_female_householder,
    universe_female_householder,
    count_male_householder,
    universe_male_householder,
    count_non_family,
    universe_non_family,
    count_under_eighteen_below_poverty,
    universe_under_eighteen_below_poverty,
    count_hh_w_children,
    universe_hh_w_children,
    count_children_five_nine_enrolled,
]
