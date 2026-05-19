from d3census import variable, Geography


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

OVERTIME_INDICATORS = [
    count_above_200_fpl,
    universe_above_200_fpl,
    count_population,
    count_population_under_18,
    count_population_over_65,
    count_home_ownership_rate,
    universe_home_ownership_rate,
    count_occupancy,
    universe_occupancy,
]