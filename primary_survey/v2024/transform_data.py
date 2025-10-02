from pathlib import Path
import configparser
import json
import pandas as pd
import geopandas as gpd
import datetime

from nvi_etl.geo_reference import pull_city_boundary, pull_council_districts, pull_zones # TODO (Mike): add a 'pull_cdo_boundaries' function
from nvi_survey import create_nvi_survey


WORKING_DIR = Path(__file__).parent
ALL = tuple() # For query string


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


def transform_data(
        logger, districts_year=2014, zones_year=2022, survey_year=2024
    ):
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
    districts = pull_council_districts(districts_year)
    zones = pull_zones(zones_year)
    # TODO Add CDO boundaries

        
    # Check that the 'pull' functions returned rows
    assert len(districts) > 0, "No districts available for the requested year."
    assert len(zones) > 0, "No zones available for the requested year."


    # 1. OPEN RAW RESPONSE AND GEOCODED FILE ---------------------------------

    survey_filename = config[f"nvi_{survey_year}_config"]["survey_responses"]
    survey_data = pd.read_csv(survey_filename, low_memory=False)

    geocoded_filename = config[f"nvi_{survey_year}_config"]["geocoded_responses"]
    geocoded = pd.read_excel(geocoded_filename)

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

    # Unless you let the boundary out a lot, you only add a few responses
    # city["geometry"] = city["geometry"].buffer(5120)

    gdf = gdf.sjoin(city[["geometry", "geoid"]], predicate="within")
    gdf["citywide"] = gdf["geoid"].map(location_map["citywide"]).astype(pd.Int64Dtype())
    gdf = gdf.drop(columns=["index_right", "geoid"])

    gdf = gdf.sjoin(districts[["geometry", "district_number"]], how="left", predicate="within")
    gdf["district"] = gdf["district_number"].map(location_map["district"]).astype(pd.Int64Dtype())
    gdf = gdf.drop(columns=["index_right", "district_number"])

    gdf = gdf.sjoin(zones[["zone_id", "geometry"]], how="left", predicate="within")
    gdf["zone"] = gdf["zone_id"].map(location_map["zone"]).astype(pd.Int64Dtype())
    gdf = gdf.drop(columns=["index_right", "zone_id"])

    
    # TODO add CDO boundaries BUT don't add it to final dataframe for website
        
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
            col: col.replace("::", ":").replace("’", "'") for col in successful_geocode.columns
        })
        .rename(columns={
            'Cleaned up or improved lot(s) <u>that I own</u>:In_The_Last_12_Months': 'Cleaned up or improved lot(s) that I own:In_The_Last_12_Months',
            'Cleaned up or improved lot(s) <u>that I do <b>not</b> own</u>:In_The_Last_12_Months': 'Cleaned up or improved lot(s) that I do not own:In_The_Last_12_Months',
            'SchoolProgram_Sports_Tutoring_Participation:Youth_In_Household_Last_12_Months_Questions4': 'SchoolProgram_Leadership_Participation:Youth_In_Household_Last_12_Months_Questions',
        })
    )

    today = datetime.date.today().strftime("%Y%m%d")

    survey_file_path = (
        WORKING_DIR / 
        "output" / 
        f"nvi_2024_analysis_source_{districts_year}_{zones_year}_{today}.csv"
    )

    recoded.to_csv(survey_file_path, index=False)

    # ROLL UP ALL INDICATORS

    nvi = create_nvi_survey(survey_file_path)

    indicators = (
        nvi.answer_key[["indicator_db_id", "response_type"]]
        .dropna(subset="indicator_db_id")
        .drop_duplicates()
    )

    result = []
    for _, indicator in indicators.iterrows():
        if indicator["response_type"] in {"GROUPED-SINGLE", "SINGLE"}:
            method = 'compile_single_response_indicator'
        elif indicator["response_type"] == "MULTI-SELECT":
            method = 'compile_multi_response_indicator'
        else:
            raise ValueError(
                f"Indicator {indicator["indicator_db_id"]}:'{indicator["response_type"]}' "
                "is not a valid response type."
            )
        aggregations = ["citywide", "district", "zone"]
        result.append(
            pd.concat(
                [
                    nvi.__getattribute__(method)(
                        indicator["indicator_db_id"], 
                        agg, 
                        readable=False
                    )
                    .reset_index()
                    .rename(columns={agg: "location_id"})
                    .assign(
                        indicator_id=indicator["indicator_db_id"],
                        year=2024
                    )
                    for agg in aggregations
                ]
            )
        )


    indicators_tall = pd.concat(result).assign(value_type_id=1)


    # ROLL UP ALL QUESTIONS

    questions = pd.concat([
        nvi.answer_key[
            nvi.answer_key["indicator_db_id"].notna()
            & (nvi.answer_key["response_type"].isin({"SINGLE", "GROUPED-SINGLE"}))
        ][["group", "question", "response_type", "indicator_db_id"]].drop_duplicates(),
        nvi.answer_key[
            nvi.answer_key["indicator_db_id"].notna()
            & (nvi.answer_key["response_type"] == "MULTI-SELECT")
        ][["group", "question", "response_type", "indicator_db_id"]].drop_duplicates(subset=["group", "response_type"])
    ])

    aggs = ["citywide", "district", "zone"]

    result = []
    for _, question in questions.iterrows():
        if question["response_type"] in {"SINGLE", "GROUPED-SINGLE"}:
            question = nvi.answer_key[
                (nvi.answer_key["group"] == question["group"])
                & (nvi.answer_key["question"] == question["question"])
            ][
                [
                    "indicator_db_id", 
                    "full_column", 
                    "survey_code", 
                    "db_question_code", 
                    "db_answer_code", 
                    "response_type",
                    "universe_query"
                ]
            ].drop_duplicates()


            for agg in aggs:
                # FIXME: This is a mess
                indicator_id = question["indicator_db_id"].iloc[0]
                survey_question_id = question["db_question_code"].iloc[0]
                column = question["full_column"].iloc[0]
                universe_query = question["universe_query"].iloc[0]

                recoder = {
                    row["survey_code"]: row["db_answer_code"]
                    for _, row in question.iterrows()
                }

                result.append(
                    nvi.survey_data
                    .query(universe_query)[[agg, column]]
                    .groupby(agg)
                    .value_counts()
                    .reset_index()
                    .rename(columns={agg: "location_id"})
                    .assign(
                        indicator_id=indicator_id,
                        survey_question_id=survey_question_id,
                        survey_question_option_id=lambda df: df[column].map(lambda v: recoder.get(v)),
                        universe=lambda df: df.groupby("location_id")["count"].transform("sum"),
                        percentage=lambda df: df["count"] / df["universe"],
                    )
                    .drop(column, axis=1)
                )
            
        elif question["response_type"] == "MULTI-SELECT":
            group = nvi.answer_key[
                nvi.answer_key["indicator_db_id"].notna()
                & (nvi.answer_key["group"] == question["group"])
            ]

            rename = {
                row["full_column"]: (row["db_question_code"], row["db_answer_code"])
                for _, row in group.iterrows()
            }

            for agg in aggs:
                result.append(
                    nvi.survey_data[[agg] + list(group["full_column"])]
                    .rename(columns=rename)
                    .groupby(agg)
                    .agg(["count", "size"])
                    .stack(level=[0,1], future_stack=True)
                    .reset_index()
                    .rename(columns={
                        agg: "location_id",
                        "level_1": "survey_question_id",
                        "level_2": "survey_question_option_id",
                        "size": "universe"
                    })
                    .assign(
                        indicator_id=question["indicator_db_id"],
                        percentage=lambda df: df["count"] / df["universe"]
                    )
                )
        else:
            raise ValueError(f"'{question["response_type"]}' is not a valid response type.")

    answers_tall = pd.concat(result).assign(value_type_id=2)

    (
        pd.concat([indicators_tall, answers_tall])
        .assign(
            year=survey_year,
            dollars=pd.NA,
            rate=pd.NA,
            rate_per=pd.NA,
            index=pd.NA,
        )
        .to_csv(WORKING_DIR / "output" / f"primary_survey_tall_{survey_year}.csv")
    )


if __name__ == "__main__":
    from nvi_etl import setup_logging

    logger = setup_logging()

    transform_data(logger)
