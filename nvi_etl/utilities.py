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

