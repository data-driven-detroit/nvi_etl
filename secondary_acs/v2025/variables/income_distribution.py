from d3census import variable, Geography


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


INCOME_DISTRIBUTION_VARIABLES = [
    income_lt_10000,
    income_10000_to_14999,
    income_15000_to_19999,
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
]
