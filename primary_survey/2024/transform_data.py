from pathlib import Path
import configparser
import os
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

WORKING_DIR = Path(__file__).parent


def assign_geo():
    

def recode(survey_data, indicator_map, logger):
    """
    The survey data has corrupted column values where sometimes it will
    be the answer index and other times it will be a text answer. 

    We have to make sure everything is not only an integer, but that the
    integer matches the answer type provided by Johnson Center.

    The indicator_map file has the 'recode' object that includes the map
    for each column.
    """

    # Recoding
    for column, recode_info in indicator_map["recode"].items():
        if column not in survey_data.columns:
            # Switching from a nested if to a guard statement + warning (MV).
            logger.warning(f"{col} not found in current dataset.")
            continue

        survey_data[column] = survey_data[column].apply(lambda x: str(int(x)) if isinstance(x, (int, float)) and x.is_integer() else str(x))
        survey_data[column] = survey_data[column].map(recode_info["mapping"])

        return survey_data


def aggregate(recoded, indicator_map, location_map, geographic_level, logger):
    """
    Aggregate the survey data to a given geography level provided the 
    rules given in the indicator_map.

    You can provide any 'geographic_level' that appears as a column on 
    the 'recoded' dataframe.
    """

    # Checking for geographic level in the indicator map config
    if geographic_level in location_map and isinstance(location_map[geographic_level], dict):
        location_mapping = location_map[geographic_level]
    else:
        logger.warning(f"Geographic level '{geographic_level}' not found in location_map.")
        return 


    results = []
    # Survey Question Level Aggregation 
    for indicator_id, indicator_info in indicator_map["indicators"].items():
        for question_id, question_info in indicator_info["questions"].items():
            question_col = question_info["column"]
            question_id_config = question_info["question_id"]
            options = question_info["options"]["values"]

            if question_col not in recoded.columns:
                logger.warning(f"'{question_col}' doesn't appear in the recoded file.")
                continue
            
            # convert to ints
            recoded[question_col] = pd.to_numeric(recoded[question_col], errors='coerce')
            recoded[question_col] = recoded[question_col].fillna(0).astype(int)

            try:
                grouped = recoded.groupby(geographic_level)[question_col]
            except KeyError as e:
                raise KeyError(f"Invalid geography level: '{geographic_level}'!")

            universe = grouped.count()

            for option_id, option_value in question_info["options"].items():
                count = grouped.apply(lambda x: sum(x.isin(option_value)))
                percentage = (count / universe * 100).fillna(0)

                for location, c, u, p in zip(universe.index, count, universe, percentage):
                    if isinstance(location_mapping, dict):
                        location_id = location_mapping.get(location, location)
                    else: 
                        location_id = location

                    results.extend([{
                            "indicator_id": indicator_id,
                            "survey_question_id": question_id,
                            "survey_question_option_id": option_id,
                            "location": location,
                            "count": c,
                            "universe": u,
                            "percentage": p,
                    }])
        
        # Indicator Level Aggregation
        indicator_cols = [q_info["column"] for q_info in indicator_info["questions"].values()]
        indicator_options = [list(q_info["options"].values()) for q_info in indicator_info["questions"].values()]

        indicator_grouped = recoded.groupby(geographic_level)[indicator_id]
        indicator_count = indicator_grouped.sum()
        indicator_universe = indicator_grouped.count()
        indicator_percentage = (indicator_count / indicator_universe * 100).fillna(0)

        for location, c, u, p in zip(indicator_universe.index, indicator_count, indicator_universe, indicator_percentage):
            if isinstance(location_mapping, dict):
                location_id = location_mapping.get(location, location)
            else: 
                location_id = location
            
            results.extend([{
                            "indicator_id": indicator_id,
                            "survey_question_id": "",
                            "survey_question_option_id": "",
                            "location": location,
                            "count": c,
                            "universe": u,
                            "percentage": p,
                        }])

    return pd.DataFrame(results)


# Transforms all indicator and question data.
def transform_data(logger):
    logger.info("Transforming data...")

    config = configparser.ConfigParser()
    config.read(WORKING_DIR / "conf" / ".conf")

    with open(WORKING_DIR / "conf" / "indicator_map.json", "r") as f:
        indicator_map = json.load(f)

    with open(WORKING_DIR / "conf" / "location_map.json", "r") as f:
        location_map = json.load(f)
        

    # Raw Survey Data
    df = pd.read_csv(config["nvi_2024_config"]["survey_responses"] , encoding="latin-1")

    # Load the Shapefiles
    zones = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/NVI Zones/nvi_neighborhood_zones_temp_2025.shp") # TODO Switch these to aux geography eventually
    city = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/City_of_Detroit_Boundary/City_of_Detroit_Boundary.shp")
    districts = gpd.read_file("P:/2024_Projects/NVI24/Development/Workspace/Abhi Workspace/Secondary Data Pull/Detroit_City_Council_Districts_2026/Detroit_City_Council_Districts_2026.shp")

    # Create GeoDataframe from CSV
    geometry = [Point(xy) for xy in zip(df['long'], df['lat])]
    gdf = gpd.GeoDataFrame(df, geometry, crs=districts.crs)

    # Joins for City, District, and Zones
    gdf = gpd.sjoin(gdf, city[['geometry', 'city_name']], how='left, predicate='within') # TODO check column name
    gdf = gdf.rename(columns={'city': 'citywide'} # the column here should be 'citywide and filled with detroit, need to check shapefile name
    gdf = gdf.drop(columns='index_right')

    gdf = gpd.sjoin(gdf, district[['geometry', 'district_number']], how='left, predicate='within')
    gdf = gdf.rename(columns='district_n': 'district_number'})
    gdf = gdf.drop(columns='index_right')

    gdf = gdp.sjoin(gdf, zones[['geom', 'zone']], how ='left', predicate='within')
    gdf = gdf.drop(columns='index_right')

    # convert back to df -- with citywide, district, and zone columns
    df = pd.DataFrame(df.drop(columns='geometry))
    
    # Recode the data to match the ids supplied by Brian
    recoded = recode(df, indicator_map, logger)
    
    logger.info(recoded.head())

    # Aggregate to the various levels
    citywide = aggregate(recoded, indicator_map, location_map, "citywide", logger)
    council_districts = aggregate(recoded, indicator_map, location_map, "district", logger)
    neighborhood_zones = aggregate(recoded, indicator_map, location_map, "zone", logger)

    # transformed_data_district.to_csv("district_test.csv", index=False)
    # Combine final dataframe
    df = pd.concat([citywide, council_districts, neighborhood_zones], ignore_index=True)
    df["survey_id"] = 1
    df["year"] = '2024'

    required_cols = [
        "survey_id",
        "year",
        "indicator_id",
        "survey_question_id",
        "survey_question_option_id",
        "location_id",
        "count",
        "universe",
        "percentage",
        "rate",
        "rate_per",
        "dollars",
        "index",
    ]

    # Add pd.NA to all the columns not in use.
    missing_cols = df.columns.symmetric_difference(required_cols)

    for col in missing_cols:
        df[col] = pd.NA

    df.to_csv(WORKING_DIR / "output" / "nvi_survey_2024.csv", index=False)
