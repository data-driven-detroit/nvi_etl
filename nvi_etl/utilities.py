from itertools import accumulate


def calculate_square_miles(gdf):
    """
    This relies on the fact the the gdf is in the NVI-standard 
    projection EPSG:2898. The unit for this projection is feet,
    so the calculation for square miles is:

    1 mile = 5,280 feet
    1 square mile = 27,878,400 (5,280²) square feet

    area sqmi = area sqft / 27,878,400 sqft/sqmi 
                            (2.788e7 as a float)

    This will throw a warning for geographic projections like web
    mercator, and also give pretty bad results.
    """
    
    return gdf.geometry.area / 2.788e7


def fix_parcel_id(parcel_id: str):
    """
    This needs to be improved, but basically a Detroit Parcel ID should
    take one of three forms:

    - 8-digits and a period: 06004253.
        - Mostly these, about 323,000 of these
    - 8-digits, a period, then a 3-digit tail, then optionally an 'L': 22037045.007, 22045086.002L
        - There are about 24,000 of these
        - Way less often the final letter can be 'A', 'B', 'C', or 'D'
    - 8-digits, a dash, then one or two digits
        - There are about 33,500 of these

    Here is the breakdown from the table raw.detodp_assessor_20221101

    -- Total parcel nums -- 381,265 | cumulative
    ------------------------------------------------
    -- '^[0-9]{8}\.$'       323,444 |  323,444
    -- '^[0-9]{8}-'          33,521 |  356,965
    -- '^[0-9]{8}\.[0-9]+'   24,285 |  381,250 
    -- '^[0-9]{9}\.[0-9]+'       11 |  381,261
    -- other                      3 |  381,264  missing addresses
    -- other                      1 |  381,265  3165 OAKMAN BLVD missing period

    """

    if "." in parcel_id:
        return parcel_id
    if "-" in parcel_id:
        return parcel_id
    return parcel_id.zfill(8) + "."



# Types for median estimator
Bin = tuple[tuple[float, float], int]
Disribution = list[Bin]


def estimate_median_from_distribution(distribution: Disribution):
    """
    # Types for median estimator
    Bin = tuple[tuple[float, float], int]
    Disribution = list[Bin]

    See type hints above. The Bin type is a tuple with the first entry
    is a tuple describing the range: [lower bound, upper bound). The 
    second tuple is a count of the number of observations in that range.

    The distribution brings several of these bins together. There is no 
    reason that these bins ought to be valid, ie ranges don't overlap or 
    cover the whole range of values, so BE CAREFUL.
    """

    if len(distribution) == 0:
        raise ValueError("The list provided must have at least one bin of type tuple[tuple[float,float], int]")

    sorted_bins = sorted(distribution)

    cumulative_sums = list(accumulate([item[1] for item in sorted_bins]))
    total_observations = cumulative_sums[-1]
    midpoint = total_observations / 2

    # len of list is -1 automatically
    target_index = len(list(filter(lambda v: v < midpoint, cumulative_sums)))
    count_below = cumulative_sums[target_index - 1]

    # Murky assumption -- uniform distribution of observations along bin values
    bin_of_concern = sorted_bins[target_index]
    count_from_bottom = midpoint - count_below
    bin_ratio = count_from_bottom / bin_of_concern[1]
    bin_bottom, bin_top = bin_of_concern[0]

    return bin_ratio * (bin_top - bin_bottom) + bin_bottom


def test_estimate_median():
    """
    The following the the test for the funciton above. It shows a consistent bias
    where the estimate is higher than the actual value. For a normal distribution
    with a sigma of 30,000 the value on average over estimates by about 17.
    """
    import random

    dataset = [int((random.gauss(100_000, 30_000))) for _ in range(100_001)]
    bins = [(i, i+10_000) for i in range(0, max(dataset), 10_000)]
    distribution = [((l,u), len(list(filter(lambda v: l <= v < u, dataset)))) for l, u in bins]
    
    actual_median = list(sorted(dataset))[len(dataset) // 2]

    return (estimate_median_from_distribution(distribution), actual_median)
