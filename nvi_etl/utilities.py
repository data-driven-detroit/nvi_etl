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
