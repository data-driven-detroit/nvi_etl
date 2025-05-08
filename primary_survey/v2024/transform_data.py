from pathlib import Path
import configparser
import json
import pandas as pd
import geopandas as gpd

from nvi_etl.geo_reference import pull_city_boundary, pull_council_districts, pull_zones


WORKING_DIR = Path(__file__).parent


def recode(survey_data, recode_map, logger):
    """
    The survey data has corrupted column values where sometimes it will
    be the answer index and other times it will be a text answer. 

    We have to make sure everything is not only an integer, but that the
    integer matches the answer type provided by Johnson Center.

    The indicator_map file has the 'recode' object that includes the map
    for each column.
    """

    def recode_val(val):
        val = str(int(val)) if isinstance(val, (int, float)) else str(val)

        # Nulls are ignored from the main call below, so return anything even
        # if its not accounted for in the recode file.
        return mapping.get(val, val)

    # Copy to avoid rewrite of old data
    survey_data = survey_data.copy()

    # Recoding
    for col, mapping in recode_map.items():
        if col not in survey_data.columns:
            # Switching from a nested if to a guard statement + warning (MV).
            logger.warning(f"{col} not found in current dataset.")
            continue
        
        try:
            survey_data[col] = (
                survey_data[col].map(recode_val, na_action="ignore").astype(pd.Int64Dtype())
            )
        except ValueError:
            logger.info(f"{col} is an as an uncoded string column!")
            survey_data[col] = survey_data[col].map(recode_val, na_action="ignore")

    return survey_data


def aggregate(recoded, indicator_map, location_map, geographic_level, logger):
    """
    Aggregate the survey data to a given geography level provided the 
    rules given in the indicator_map.

    You can provide any 'geographic_level' that appears as a column on 
    the 'recoded' dataframe.
    """

    # Checking for geographic level in the indicator map config
    location_mapping = location_map[geographic_level]

    results = []
    # Survey Question Level Aggregation 
    for indicator_id, indicator_info in indicator_map["indicators"].items():
        for question_id, question_info in indicator_info["questions"].items():
            question_col = question_info["column"]

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
                    location_id = location_mapping.get(location, location)

                    results.extend([{
                            "indicator_id": indicator_id,
                            "survey_question_id": question_id,
                            "survey_question_option_id": option_id,
                            "location": location_id,
                            "count": c,
                            "universe": u,
                            "percentage": p,
                    }])
        
        # Indicator Level Aggregation
        indicator_cols = [
            q_info["column"] for q_info in indicator_info["questions"].values()
        ]
        indicator_options = [
            list(q_info["options"].values()) 
            for q_info in indicator_info["questions"].values()
        ]

        indicator_grouped = recoded.groupby(geographic_level)[indicator_cols]
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

def transform_data(logger, districts_year=2026, zones_year=2026):
    """
    Transform table of contents:

    0. Load all shapefiles and aggregation metadata
    1. Open raw response and geocoded file
    2. Append correct council districts and zones for analysis
        - Source analysis file 2026 Council Districts and Neighborhood Zones
        - For rest of the ETL -- 2026 Council Districts and Neighborhood Zones
        - For ARPA analysis -- 2014 Council Districts and 2022 Neighborhood Zones
    3. Recode indicators to make sure they match output **perfectly**
    """

    # 0. LOAD ALL SHAPEFILES AND AGGREGATION METADATA ------------------------

    logger.info("Transforming survey and geocoded file.")

    config = configparser.ConfigParser()
    config.read(WORKING_DIR / "conf" / ".conf")

    indicator_map = json.loads(
        (WORKING_DIR / "conf" / "indicator_map.json").read_text()
    )

    location_map = json.loads(
        (WORKING_DIR / "conf" / "location_map.json").read_text()
    )
        
    city = pull_city_boundary()
    districts = pull_council_districts(2014)
    zones = pull_zones(2022)

    # Check that the 'pull' functions returned rows
    assert len(districts) > 0, "No districts available for the requested year."
    assert len(zones) > 0, "No zones available for the requested year."


    # 1. OPEN RAW RESPONSE AND GEOCODED FILE ---------------------------------

    survey_data = pd.read_csv(
        config["nvi_2024_config"]["survey_responses"], 
        low_memory=False
    )

    geocoded = pd.read_excel(config["nvi_2024_config"]["geocoded_responses"])

    if len(geocoded) != len(survey_data):
        logger.wanring(f"# geocoded rows don't match the original survey data!")

    # Don't use any survey data from the geocoded file!
    merged = survey_data.merge(
        geocoded[["USER_Response_ID", "Status", "X", "Y"]]
        .rename(columns={"Status": "successful_geocode"}), 
        left_on="Response ID", right_on="USER_Response_ID"
    )

    # Create the geodataframe in the D3-standard projection 'EPSG:2898'
    gdf = gpd.GeoDataFrame(
        merged, 
        geometry=gpd.points_from_xy(merged["X"], merged["Y"]), 
        crs="EPSG:4326"
    ).to_crs("EPSG:2898")


    # 2. JOINS FOR CITY, DISTRICT, AND ZONES ---------------------------------

    gdf = gdf.sjoin(city[["geometry", "geoid"]], predicate="within")
    gdf["citywide"] = gdf["geoid"].map(location_map["citywide"]).astype(pd.Int64Dtype())
    gdf = gdf.drop(columns=["index_right", "geoid"])

    gdf = gdf.sjoin(districts[["geometry", "district_number"]], how="left", predicate="within")
    gdf["district"] = gdf["district_number"].map(location_map["district"]).astype(pd.Int64Dtype())
    gdf = gdf.drop(columns=["index_right", "district_number"])

    gdf = gdf.sjoin(zones[["zone_id", "geometry"]], how="left", predicate="within")
    gdf["zone"] = gdf["zone_id"].map(location_map["zone"]).astype(pd.Int64Dtype())
    gdf = gdf.drop(columns=["index_right", "zone_id"])

    # convert back to df -- with citywide, district, and zone columns
    df = pd.DataFrame(gdf.drop(columns='geometry'))
    df = df.drop_duplicates(subset="Response ID")


    # 2-B. CHECK FOR DUPLICATED ROWS AND OTHER ERRORS ------------------------

    if (dups := len(gdf) - len(survey_data)) > 0:
        logger.warning(
            f"Geography joins are causing {dups} duplicated row(s) -- check "
            "for overlapping boundaries!"
        )

    # Check for geojoining dropping rows
    elif (drops := len(survey_data) - len(gdf)) > 0:
        logger.warning(
            f"Geography joins are causing {drops} dropped row(s) -- check for "
            "points outside bounds!"
        )

    # Only include successful_geocoding
    successful_geocode = df[df["successful_geocode"] == "M"]

    n_excluded = len(df) - len(successful_geocode)
    logger.warning(f"{n_excluded} records excluded due to geocoding error.")
    logger.info(f"{len(successful_geocode)} rows ready for further analysis.")


    # 3. RECODE INDICATORS TO MAKE SURE THEY MATCH OUTPUT **PERFECTLY** ------

    recode_map = json.loads((WORKING_DIR / "conf" / "recode.json").read_text())
    recoded = (
        recode(successful_geocode, recode_map, logger)
        .rename(columns={
            col: col.replace("::", ":") for col in successful_geocode.columns
        })
        .rename(columns={
            'Cleaned up or improved lot(s) <u>that I own</u>:In_The_Last_12_Months': 'Cleaned up or improved lot(s) that I own:In_The_Last_12_Months',
            'Cleaned up or improved lot(s) <u>that I do <b>not</b> own</u>:In_The_Last_12_Months': 'Cleaned up or improved lot(s) that I do not own:In_The_Last_12_Months',
            # 'SchoolProgram_Sports_Tutoring_Participation:Youth_In_Household_Last_12_Months_Questions4': 'SchoolProgram_Sports_Tutoring_Participation:Youth_In_Household_Last_12_Months_Questions',
        })
    )

    recoded.to_csv(WORKING_DIR / "output" / "nvi_2024_analysis_source.csv", index=False)


    # 4. CREATE QUESTION-LEVEL WIDE TABLE ------------------------------------
    # 5. ROLL UP QUESTIONS INTO INDICATORS -----------------------------------
    # 5. SAVE NECESSARY ROLL UPS FOR FURTHER ANALYSIS ------------------------
    # 6. FLIP WIDE TO LONG FOR DATABASE --------------------------------------


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform_data(logger)