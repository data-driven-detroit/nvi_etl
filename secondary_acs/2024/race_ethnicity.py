from d3census import  variable, Geography


# Race / Ethnicity

@variable
def count_aian(geo: Geography):
    return geo.B03002._005E


@variable
def universe_aian(geo: Geography):
    return geo.B03002._001E


@variable
def count_asian(geo: Geography):
    return geo.B03002._006E


@variable
def universe_asian(geo: Geography):
    return geo.B03002._001E


@variable
def count_black(geo: Geography):
    return geo.B03002._004E


@variable
def universe_black(geo: Geography):
    return geo.B03002._001E


@variable
def count_hispanic_latino(geo: Geography):
    return geo.B03002._012E


@variable
def universe_hispanic_latino(geo: Geography):
    return geo.B03002._001E


@variable
def count_nhpi(geo: Geography):
    return geo.B03002._007E


@variable
def universe_nhpi(geo: Geography):
    return geo.B03002._007E


@variable
def count_white(geo: Geography):
    return geo.B03002._003E


@variable
def universe_white(geo: Geography):
    return geo.B03002._001E


@variable
def count_other_race(geo: Geography):
    return geo.B03002._008E


@variable
def universe_other_race(geo: Geography):
    return geo.B03002._001E


@variable
def count_two_plus_races(geo: Geography):
    return geo.B03002._009E


@variable
def universe_two_plus_races(geo: Geography):
    return geo.B03002._001E



RACE_ETHNICITY_VARIABLES = [
    count_aian,
    universe_aian,
    count_asian,
    universe_asian,
    count_black,
    universe_black,
    count_hispanic_latino,
    universe_hispanic_latino,
    count_nhpi,
    universe_nhpi,
    count_white,
    universe_white,
    count_other_race,
    universe_other_race,
    count_two_plus_races,
    universe_two_plus_races,
]
