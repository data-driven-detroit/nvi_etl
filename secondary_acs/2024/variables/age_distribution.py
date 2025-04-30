from d3census import variable, Geography


@variable
def count_age_under_five(geo: Geography):
    return sum([geo.B01001._003E, geo.B01001._027E])


@variable
def universe_age_under_five(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_five_to_nine(geo: Geography):
    return sum([geo.B01001._004E, geo.B01001._028E])


@variable
def universe_age_five_to_nine(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_ten_to_fourteen(geo: Geography):
    return sum(
        [  # type: ignore
            geo.B01001._005E,
            geo.B01001._029E,
        ]
    )


@variable
def universe_age_ten_to_fourteen(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_fifteen_to_nineteen(geo: Geography):
    return sum(
        [
            geo.B01001._006E,
            geo.B01001._007E,
            geo.B01001._030E,
            geo.B01001._031E,
        ]
    )


@variable
def universe_age_fifteen_to_nineteen(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_twenty_to_twentyfour(geo: Geography):
    return sum(
        [
            geo.B01001._008E,
            geo.B01001._009E,
            geo.B01001._010E,
            geo.B01001._032E,
            geo.B01001._033E,
            geo.B01001._034E,
        ]
    )


@variable
def universe_age_twenty_to_twentyfour(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_twentyfive_to_twenty_nine(geo: Geography):
    return sum([geo.B01001._011E, geo.B01001._035E])


@variable
def universe_age_twentyfive_to_twenty_nine(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_thirty_to_thirtyfour(geo: Geography):
    return sum([geo.B01001._012E, geo.B01001._036E])


@variable
def universe_age_thirty_to_thirtyfour(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_thirtyfive_to_thirtynine(geo: Geography):
    return sum([geo.B01001._013E, geo.B01001._037E])


@variable
def universe_age_thirtyfive_to_thirtynine(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_fourty_to_fourtyfour(geo: Geography):
    return sum([geo.B01001._014E, geo.B01001._038E])


@variable
def universe_age_fourty_to_fourtyfour(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_fourtyfive_to_fourtynine(geo: Geography):
    return sum([geo.B01001._015E, geo.B01001._039E])


@variable
def universe_age_fourtyfive_to_fourtynine(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_fifty_to_fiftyfour(geo: Geography):
    return sum([geo.B01001._016E, geo.B01001._040E])


@variable
def universe_age_fifty_to_fiftyfour(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_fiftyfive_to_fiftynine(geo: Geography):
    return sum([geo.B01001._017E, geo.B01001._041E])


@variable
def universe_age_fiftyfive_to_fiftynine(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_sixty_to_sixtyfour(geo: Geography):
    return sum(
        [geo.B01001._018E, geo.B01001._019E, geo.B01001._042E, geo.B01001._043E]
    )


@variable
def universe_age_sixty_to_sixtyfour(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_sixtyfive_to_sixtynine(geo: Geography):
    return sum(
        [geo.B01001._020E, geo.B01001._021E, geo.B01001._044E, geo.B01001._045E]
    )


@variable
def universe_age_sixtyfive_to_sixtynine(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_seventy_to_seventyfour(geo: Geography):
    return sum(
        [
            geo.B01001._022E,
            geo.B01001._046E,
        ]
    )


@variable
def universe_age_seventy_to_seventyfour(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_seventyfive_to_seventynine(geo: Geography):
    return sum(
        [
            geo.B01001._023E,
            geo.B01001._047E,
        ]
    )


@variable
def universe_age_seventyfive_to_seventynine(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_eighty_to_eightyfour(geo: Geography):
    return sum(
        [
            geo.B01001._024E,
            geo.B01001._048E,
        ]
    )


@variable
def universe_age_eighty_to_eightyfour(geo: Geography):
    return geo.B01001._001E


@variable
def count_age_eightyfive_and_up(geo: Geography):
    return sum(
        [
            geo.B01001._025E,
            geo.B01001._049E,
        ]
    )


@variable
def universe_age_eightyfive_and_up(geo: Geography):
    return geo.B01001._001E


AGE_DISTRIBUTION_VARIABLES = [
    count_age_under_five,
    universe_age_under_five,
    count_age_five_to_nine,
    universe_age_five_to_nine,
    count_age_ten_to_fourteen,
    universe_age_ten_to_fourteen,
    count_age_fifteen_to_nineteen,
    universe_age_fifteen_to_nineteen,
    count_age_twenty_to_twentyfour,
    universe_age_twenty_to_twentyfour,
    count_age_twentyfive_to_twenty_nine,
    universe_age_twentyfive_to_twenty_nine,
    count_age_thirty_to_thirtyfour,
    universe_age_thirty_to_thirtyfour,
    count_age_thirtyfive_to_thirtynine,
    universe_age_thirtyfive_to_thirtynine,
    count_age_fourty_to_fourtyfour,
    universe_age_fourty_to_fourtyfour,
    count_age_fourtyfive_to_fourtynine,
    universe_age_fourtyfive_to_fourtynine,
    count_age_fifty_to_fiftyfour,
    universe_age_fifty_to_fiftyfour,
    count_age_fiftyfive_to_fiftynine,
    universe_age_fiftyfive_to_fiftynine,
    count_age_sixty_to_sixtyfour,
    universe_age_sixty_to_sixtyfour,
    count_age_sixtyfive_to_sixtynine,
    universe_age_sixtyfive_to_sixtynine,
    count_age_seventy_to_seventyfour,
    universe_age_seventy_to_seventyfour,
    count_age_seventyfive_to_seventynine,
    universe_age_seventyfive_to_seventynine,
    count_age_eighty_to_eightyfour,
    universe_age_eighty_to_eightyfour,
    count_age_eightyfive_and_up,
    universe_age_eightyfive_and_up,
]
