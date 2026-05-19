"""ACS variable definitions for d3census.

Merged from individual variable files: age_distribution, gross_rent_distribution,
home_value_distribution, income_distribution, other_indicators, over_time,
race_ethnicity.
"""

from d3census import variable, Geography


# =============================================================================
# Age Distribution (B01001)
# =============================================================================

@variable
def count_age_under_five(geo: Geography):
    return sum([geo.B01001._003E, geo.B01001._027E])

@variable
def count_lt_eighteen_dist_under_five(geo: Geography):
    return sum([geo.B01001._003E, geo.B01001._027E])

@variable
def universe_age_under_five(geo: Geography):
    return geo.B01001._001E

@variable
def universe_lt_eighteen_dist_under_five(geo: Geography):
    return sum([
        geo.B01001._003E, geo.B01001._004E, geo.B01001._005E, geo.B01001._006E,
        geo.B01001._027E, geo.B01001._028E, geo.B01001._029E, geo.B01001._030E,
    ])

@variable
def count_age_five_to_nine(geo: Geography):
    return sum([geo.B01001._004E, geo.B01001._028E])

@variable
def count_lt_eighteen_dist_five_to_nine(geo: Geography):
    return sum([geo.B01001._004E, geo.B01001._028E])

@variable
def universe_age_five_to_nine(geo: Geography):
    return geo.B01001._001E

@variable
def universe_lt_eighteen_dist_five_to_nine(geo: Geography):
    return sum([
        geo.B01001._003E, geo.B01001._004E, geo.B01001._005E, geo.B01001._006E,
        geo.B01001._027E, geo.B01001._028E, geo.B01001._029E, geo.B01001._030E,
    ])

@variable
def count_age_ten_to_fourteen(geo: Geography):
    return sum([geo.B01001._005E, geo.B01001._029E])

@variable
def count_lt_eighteen_dist_ten_to_fourteen(geo: Geography):
    return sum([geo.B01001._005E, geo.B01001._029E])

@variable
def universe_age_ten_to_fourteen(geo: Geography):
    return geo.B01001._001E

@variable
def universe_lt_eighteen_dist_ten_to_fourteen(geo: Geography):
    return sum([
        geo.B01001._003E, geo.B01001._004E, geo.B01001._005E, geo.B01001._006E,
        geo.B01001._027E, geo.B01001._028E, geo.B01001._029E, geo.B01001._030E,
    ])

@variable
def count_age_fifteen_to_nineteen(geo: Geography):
    return sum([
        geo.B01001._006E, geo.B01001._007E,
        geo.B01001._030E, geo.B01001._031E,
    ])

@variable
def count_lt_eighteen_dist_fifteen_seventeen(geo: Geography):
    return sum([geo.B01001._006E, geo.B01001._030E])

@variable
def universe_lt_eighteen_dist_fifteen_seventeen(geo: Geography):
    return sum([
        geo.B01001._003E, geo.B01001._004E, geo.B01001._005E, geo.B01001._006E,
        geo.B01001._027E, geo.B01001._028E, geo.B01001._029E, geo.B01001._030E,
    ])

@variable
def universe_age_fifteen_to_nineteen(geo: Geography):
    return geo.B01001._001E

@variable
def count_age_twenty_to_twentyfour(geo: Geography):
    return sum([
        geo.B01001._008E, geo.B01001._009E, geo.B01001._010E,
        geo.B01001._032E, geo.B01001._033E, geo.B01001._034E,
    ])

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
    return sum([geo.B01001._018E, geo.B01001._019E, geo.B01001._042E, geo.B01001._043E])

@variable
def universe_age_sixty_to_sixtyfour(geo: Geography):
    return geo.B01001._001E

@variable
def count_age_sixtyfive_to_sixtynine(geo: Geography):
    return sum([geo.B01001._020E, geo.B01001._021E, geo.B01001._044E, geo.B01001._045E])

@variable
def universe_age_sixtyfive_to_sixtynine(geo: Geography):
    return geo.B01001._001E

@variable
def count_age_seventy_to_seventyfour(geo: Geography):
    return sum([geo.B01001._022E, geo.B01001._046E])

@variable
def universe_age_seventy_to_seventyfour(geo: Geography):
    return geo.B01001._001E

@variable
def count_age_seventyfive_to_seventynine(geo: Geography):
    return sum([geo.B01001._023E, geo.B01001._047E])

@variable
def universe_age_seventyfive_to_seventynine(geo: Geography):
    return geo.B01001._001E

@variable
def count_age_eighty_to_eightyfour(geo: Geography):
    return sum([geo.B01001._024E, geo.B01001._048E])

@variable
def universe_age_eighty_to_eightyfour(geo: Geography):
    return geo.B01001._001E

@variable
def count_age_eightyfive_and_up(geo: Geography):
    return sum([geo.B01001._025E, geo.B01001._049E])

@variable
def universe_age_eightyfive_and_up(geo: Geography):
    return geo.B01001._001E


AGE_DISTRIBUTION_VARIABLES = [
    count_age_under_five, universe_age_under_five,
    count_age_five_to_nine, universe_age_five_to_nine,
    count_age_ten_to_fourteen, universe_age_ten_to_fourteen,
    count_age_fifteen_to_nineteen, universe_age_fifteen_to_nineteen,
    count_age_twenty_to_twentyfour, universe_age_twenty_to_twentyfour,
    count_age_twentyfive_to_twenty_nine, universe_age_twentyfive_to_twenty_nine,
    count_age_thirty_to_thirtyfour, universe_age_thirty_to_thirtyfour,
    count_age_thirtyfive_to_thirtynine, universe_age_thirtyfive_to_thirtynine,
    count_age_fourty_to_fourtyfour, universe_age_fourty_to_fourtyfour,
    count_age_fourtyfive_to_fourtynine, universe_age_fourtyfive_to_fourtynine,
    count_age_fifty_to_fiftyfour, universe_age_fifty_to_fiftyfour,
    count_age_fiftyfive_to_fiftynine, universe_age_fiftyfive_to_fiftynine,
    count_age_sixty_to_sixtyfour, universe_age_sixty_to_sixtyfour,
    count_age_sixtyfive_to_sixtynine, universe_age_sixtyfive_to_sixtynine,
    count_age_seventy_to_seventyfour, universe_age_seventy_to_seventyfour,
    count_age_seventyfive_to_seventynine, universe_age_seventyfive_to_seventynine,
    count_age_eighty_to_eightyfour, universe_age_eighty_to_eightyfour,
    count_age_eightyfive_and_up, universe_age_eightyfive_and_up,
    count_lt_eighteen_dist_under_five, universe_lt_eighteen_dist_under_five,
    count_lt_eighteen_dist_five_to_nine, universe_lt_eighteen_dist_five_to_nine,
    count_lt_eighteen_dist_ten_to_fourteen, universe_lt_eighteen_dist_ten_to_fourteen,
    count_lt_eighteen_dist_fifteen_seventeen, universe_lt_eighteen_dist_fifteen_seventeen,
]


# =============================================================================
# Gross Rent Distribution (B25063)
# =============================================================================

@variable
def gross_rent_1(geo: Geography): return geo.B25063._003E
@variable
def gross_rent_2(geo: Geography): return geo.B25063._004E
@variable
def gross_rent_3(geo: Geography): return geo.B25063._005E
@variable
def gross_rent_4(geo: Geography): return geo.B25063._006E
@variable
def gross_rent_5(geo: Geography): return geo.B25063._007E
@variable
def gross_rent_6(geo: Geography): return geo.B25063._008E
@variable
def gross_rent_7(geo: Geography): return geo.B25063._009E
@variable
def gross_rent_8(geo: Geography): return geo.B25063._010E
@variable
def gross_rent_9(geo: Geography): return geo.B25063._011E
@variable
def gross_rent_10(geo: Geography): return geo.B25063._012E
@variable
def gross_rent_11(geo: Geography): return geo.B25063._013E
@variable
def gross_rent_12(geo: Geography): return geo.B25063._014E
@variable
def gross_rent_13(geo: Geography): return geo.B25063._015E
@variable
def gross_rent_14(geo: Geography): return geo.B25063._016E
@variable
def gross_rent_15(geo: Geography): return geo.B25063._017E
@variable
def gross_rent_16(geo: Geography): return geo.B25063._018E
@variable
def gross_rent_17(geo: Geography): return geo.B25063._019E
@variable
def gross_rent_18(geo: Geography): return geo.B25063._020E
@variable
def gross_rent_19(geo: Geography): return geo.B25063._021E
@variable
def gross_rent_20(geo: Geography): return geo.B25063._022E
@variable
def gross_rent_21(geo: Geography): return geo.B25063._023E
@variable
def gross_rent_22(geo: Geography): return geo.B25063._024E
@variable
def gross_rent_23(geo: Geography): return geo.B25063._025E
@variable
def gross_rent_24(geo: Geography): return geo.B25063._026E

GROSS_RENT_DISTRIBUTION = [
    gross_rent_1, gross_rent_2, gross_rent_3, gross_rent_4,
    gross_rent_5, gross_rent_6, gross_rent_7, gross_rent_8,
    gross_rent_9, gross_rent_10, gross_rent_11, gross_rent_12,
    gross_rent_13, gross_rent_14, gross_rent_15, gross_rent_16,
    gross_rent_17, gross_rent_18, gross_rent_19, gross_rent_20,
    gross_rent_21, gross_rent_22, gross_rent_23, gross_rent_24,
]


# =============================================================================
# Home Value Distribution (B25075)
# =============================================================================

@variable
def oo_value_dist_1(geo: Geography): return geo.B25075._002E
@variable
def oo_value_dist_2(geo: Geography): return geo.B25075._003E
@variable
def oo_value_dist_3(geo: Geography): return geo.B25075._004E
@variable
def oo_value_dist_4(geo: Geography): return geo.B25075._005E
@variable
def oo_value_dist_5(geo: Geography): return geo.B25075._006E
@variable
def oo_value_dist_6(geo: Geography): return geo.B25075._007E
@variable
def oo_value_dist_7(geo: Geography): return geo.B25075._008E
@variable
def oo_value_dist_8(geo: Geography): return geo.B25075._009E
@variable
def oo_value_dist_9(geo: Geography): return geo.B25075._010E
@variable
def oo_value_dist_10(geo: Geography): return geo.B25075._011E
@variable
def oo_value_dist_11(geo: Geography): return geo.B25075._012E
@variable
def oo_value_dist_12(geo: Geography): return geo.B25075._013E
@variable
def oo_value_dist_13(geo: Geography): return geo.B25075._014E
@variable
def oo_value_dist_14(geo: Geography): return geo.B25075._015E
@variable
def oo_value_dist_15(geo: Geography): return geo.B25075._016E
@variable
def oo_value_dist_16(geo: Geography): return geo.B25075._017E
@variable
def oo_value_dist_17(geo: Geography): return geo.B25075._018E
@variable
def oo_value_dist_18(geo: Geography): return geo.B25075._019E
@variable
def oo_value_dist_19(geo: Geography): return geo.B25075._020E
@variable
def oo_value_dist_20(geo: Geography): return geo.B25075._021E
@variable
def oo_value_dist_21(geo: Geography): return geo.B25075._022E
@variable
def oo_value_dist_22(geo: Geography): return geo.B25075._023E
@variable
def oo_value_dist_23(geo: Geography): return geo.B25075._024E
@variable
def oo_value_dist_24(geo: Geography): return geo.B25075._025E
@variable
def oo_value_dist_25(geo: Geography): return geo.B25075._026E
@variable
def oo_value_dist_26(geo: Geography): return geo.B25075._027E

HOME_VALUE_DISTRIBUTION_VARIABLES = [
    oo_value_dist_1, oo_value_dist_2, oo_value_dist_3, oo_value_dist_4,
    oo_value_dist_5, oo_value_dist_6, oo_value_dist_7, oo_value_dist_8,
    oo_value_dist_9, oo_value_dist_10, oo_value_dist_11, oo_value_dist_12,
    oo_value_dist_13, oo_value_dist_14, oo_value_dist_15, oo_value_dist_16,
    oo_value_dist_17, oo_value_dist_18, oo_value_dist_19, oo_value_dist_20,
    oo_value_dist_21, oo_value_dist_22, oo_value_dist_23, oo_value_dist_24,
    oo_value_dist_25, oo_value_dist_26,
]


# =============================================================================
# Income Distribution (B19001)
# =============================================================================

@variable
def income_lt_10000(geo: Geography): return geo.B19001._002E
@variable
def income_10000_to_14999(geo: Geography): return geo.B19001._003E
@variable
def income_15000_to_19999(geo: Geography): return geo.B19001._004E
@variable
def income_20000_to_24999(geo: Geography): return geo.B19001._005E
@variable
def income_25000_to_29999(geo: Geography): return geo.B19001._006E
@variable
def income_30000_to_34999(geo: Geography): return geo.B19001._007E
@variable
def income_35000_to_39999(geo: Geography): return geo.B19001._008E
@variable
def income_40000_to_44999(geo: Geography): return geo.B19001._009E
@variable
def income_45000_to_49999(geo: Geography): return geo.B19001._010E
@variable
def income_50000_to_59999(geo: Geography): return geo.B19001._011E
@variable
def income_60000_to_74999(geo: Geography): return geo.B19001._012E
@variable
def income_75000_to_99999(geo: Geography): return geo.B19001._013E
@variable
def income_100000_to_124999(geo: Geography): return geo.B19001._014E
@variable
def income_125000_to_149999(geo: Geography): return geo.B19001._015E
@variable
def income_150000_to_199999(geo: Geography): return geo.B19001._016E
@variable
def income_ge_200000(geo: Geography): return geo.B19001._017E

INCOME_DISTRIBUTION_VARIABLES = [
    income_lt_10000, income_10000_to_14999, income_15000_to_19999,
    income_20000_to_24999, income_25000_to_29999, income_30000_to_34999,
    income_35000_to_39999, income_40000_to_44999, income_45000_to_49999,
    income_50000_to_59999, income_60000_to_74999, income_75000_to_99999,
    income_100000_to_124999, income_125000_to_149999, income_150000_to_199999,
    income_ge_200000,
]


# =============================================================================
# Other Indicators (B15003, B25106, B23001, B11001, B17020, B11005, B14003)
# =============================================================================

@variable
def count_hs_diploma(geo: Geography):
    return sum([
        geo.B15003._017E, geo.B15003._018E, geo.B15003._019E,
        geo.B15003._020E, geo.B15003._021E, geo.B15003._022E,
        geo.B15003._023E, geo.B15003._024E, geo.B15003._025E,
    ])

@variable
def universe_hs_diploma(geo: Geography):
    return geo.B15003._001E

@variable
def count_postsecondary(geo: Geography):
    return sum([
        geo.B15003._021E, geo.B15003._022E, geo.B15003._023E,
        geo.B15003._024E, geo.B15003._025E,
    ])

@variable
def universe_postsecondary(geo: Geography):
    return geo.B15003._001E

@variable
def count_oo_no_cost_burden(geo: Geography):
    return sum([
        geo.B25106._004E, geo.B25106._005E, geo.B25106._008E,
        geo.B25106._009E, geo.B25106._012E, geo.B25106._013E,
        geo.B25106._016E, geo.B25106._017E, geo.B25106._020E,
        geo.B25106._021E,
    ])

@variable
def universe_oo_no_cost_burden(geo: Geography):
    return geo.B25003._002E

@variable
def count_ro_no_cost_burden(geo: Geography):
    return sum([
        geo.B25106._026E, geo.B25106._027E, geo.B25106._030E,
        geo.B25106._031E, geo.B25106._034E, geo.B25106._035E,
        geo.B25106._038E, geo.B25106._039E, geo.B25106._042E,
        geo.B25106._043E,
    ])

@variable
def universe_ro_no_cost_burden(geo: Geography):
    return geo.B25106._024E

@variable
def count_over_20_seeking_emp(geo: Geography):
    return sum([
        geo.B23001._011E, geo.B23001._018E, geo.B23001._025E,
        geo.B23001._032E, geo.B23001._039E, geo.B23001._046E,
        geo.B23001._053E, geo.B23001._060E, geo.B23001._067E,
        geo.B23001._074E, geo.B23001._079E, geo.B23001._084E,
        geo.B23001._097E, geo.B23001._104E, geo.B23001._111E,
        geo.B23001._118E, geo.B23001._125E, geo.B23001._132E,
        geo.B23001._139E, geo.B23001._146E, geo.B23001._153E,
        geo.B23001._160E, geo.B23001._165E, geo.B23001._170E,
    ])

@variable
def universe_over_20_seeking_emp(geo: Geography):
    return sum([
        geo.B23001._010E, geo.B23001._017E, geo.B23001._024E,
        geo.B23001._031E, geo.B23001._038E, geo.B23001._045E,
        geo.B23001._052E, geo.B23001._059E, geo.B23001._066E,
        geo.B23001._073E, geo.B23001._078E, geo.B23001._083E,
        geo.B23001._096E, geo.B23001._103E, geo.B23001._110E,
        geo.B23001._117E, geo.B23001._124E, geo.B23001._131E,
        geo.B23001._138E, geo.B23001._145E, geo.B23001._152E,
        geo.B23001._159E, geo.B23001._164E, geo.B23001._169E,
    ])

@variable
def count_btwn_16_19_seeking_emp(geo: Geography):
    return sum([geo.B23001._004E, geo.B23001._090E])

@variable
def universe_btwn_16_19_seeking_emp(geo: Geography):
    return sum([geo.B23001._003E, geo.B23001._089E])

@variable
def count_married_couples(geo: Geography):
    return geo.B11001._003E

@variable
def universe_married_couples(geo: Geography):
    return geo.B11001._001E

@variable
def count_female_householder(geo: Geography):
    return geo.B11001._006E

@variable
def universe_female_householder(geo: Geography):
    return geo.B11001._001E

@variable
def count_male_householder(geo: Geography):
    return geo.B11001._005E

@variable
def universe_male_householder(geo: Geography):
    return geo.B11001._001E

@variable
def count_non_family(geo: Geography):
    return geo.B11001._007E

@variable
def universe_non_family(geo: Geography):
    return geo.B11001._001E

@variable
def count_under_eighteen_below_poverty(geo: Geography):
    return sum([geo.B17020._003E, geo.B17020._004E, geo.B17020._005E])

@variable
def universe_under_eighteen_below_poverty(geo: Geography):
    return sum([
        geo.B17020._003E, geo.B17020._004E, geo.B17020._005E,
        geo.B17020._011E, geo.B17020._012E, geo.B17020._013E,
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
        geo.B14003._005E, geo.B14003._014E,
        geo.B14003._033E, geo.B14003._042E,
    ])

OTHER_INDICATORS = [
    count_hs_diploma, universe_hs_diploma,
    count_postsecondary, universe_postsecondary,
    count_oo_no_cost_burden, universe_oo_no_cost_burden,
    count_ro_no_cost_burden, universe_ro_no_cost_burden,
    count_over_20_seeking_emp, universe_over_20_seeking_emp,
    count_btwn_16_19_seeking_emp, universe_btwn_16_19_seeking_emp,
    count_married_couples, universe_married_couples,
    count_female_householder, universe_female_householder,
    count_male_householder, universe_male_householder,
    count_non_family, universe_non_family,
    count_under_eighteen_below_poverty, universe_under_eighteen_below_poverty,
    count_hh_w_children, universe_hh_w_children,
    count_children_five_nine_enrolled,
]


# =============================================================================
# Over Time Indicators (C17002, B01003, B01001, B25003, B25002)
# =============================================================================

@variable
def count_above_200_fpl(geo: Geography):
    return geo.C17002._008E

@variable
def universe_above_200_fpl(geo: Geography):
    return geo.C17002._001E

@variable
def count_population(geo: Geography):
    return geo.B01003._001E

@variable
def count_population_under_18(geo: Geography):
    return sum([
        geo.B01001._003E, geo.B01001._004E, geo.B01001._005E, geo.B01001._006E,
        geo.B01001._027E, geo.B01001._028E, geo.B01001._029E, geo.B01001._030E,
    ])

@variable
def count_population_over_65(geo: Geography):
    return sum([
        geo.B01001._020E, geo.B01001._021E, geo.B01001._022E,
        geo.B01001._023E, geo.B01001._024E, geo.B01001._025E,
        geo.B01001._044E, geo.B01001._045E, geo.B01001._046E,
        geo.B01001._047E, geo.B01001._048E, geo.B01001._049E,
    ])

@variable
def count_home_ownership_rate(geo: Geography):
    return geo.B25003._002E

@variable
def universe_home_ownership_rate(geo: Geography):
    return geo.B25003._001E

@variable
def count_occupancy(geo: Geography):
    return geo.B25003._001E

@variable
def universe_occupancy(geo: Geography):
    return geo.B25002._001E

OVERTIME_INDICATORS = [
    count_above_200_fpl, universe_above_200_fpl,
    count_population,
    count_population_under_18,
    count_population_over_65,
    count_home_ownership_rate, universe_home_ownership_rate,
    count_occupancy, universe_occupancy,
]


# =============================================================================
# Race / Ethnicity (B03002)
# =============================================================================

@variable
def count_aian(geo: Geography): return geo.B03002._005E
@variable
def universe_aian(geo: Geography): return geo.B03002._001E
@variable
def count_asian(geo: Geography): return geo.B03002._006E
@variable
def universe_asian(geo: Geography): return geo.B03002._001E
@variable
def count_black(geo: Geography): return geo.B03002._004E
@variable
def universe_black(geo: Geography): return geo.B03002._001E
@variable
def count_hispanic_latino(geo: Geography): return geo.B03002._012E
@variable
def universe_hispanic_latino(geo: Geography): return geo.B03002._001E
@variable
def count_nhpi(geo: Geography): return geo.B03002._007E
@variable
def universe_nhpi(geo: Geography): return geo.B03002._001E
@variable
def count_white(geo: Geography): return geo.B03002._003E
@variable
def universe_white(geo: Geography): return geo.B03002._001E
@variable
def count_other_race(geo: Geography): return geo.B03002._008E
@variable
def universe_other_race(geo: Geography): return geo.B03002._001E
@variable
def count_two_plus_races(geo: Geography): return geo.B03002._009E
@variable
def universe_two_plus_races(geo: Geography): return geo.B03002._001E

RACE_ETHNICITY_VARIABLES = [
    count_aian, universe_aian,
    count_asian, universe_asian,
    count_black, universe_black,
    count_hispanic_latino, universe_hispanic_latino,
    count_nhpi, universe_nhpi,
    count_white, universe_white,
    count_other_race, universe_other_race,
    count_two_plus_races, universe_two_plus_races,
]
