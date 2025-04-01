from pathlib import Path
import configparser
import os
import json
import pandas as pd

WORKING_DIR = Path(__file__).parent

# This was commented out, check reviewing to see if this is captured in the later aggregation step?
# Columns that need to be combined
# for combined_col, cols_to_combine in config['combine_columns'].items():
#     survey_data[combined_col] = survey_data[cols_to_combine].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)
#     survey_data.drop(columns=cols_to_combine, inplace = True)

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
    recoded_df = survey_data.copy()
    
    for col, recode_info in indicator_map["recode"].items():
        if col not in survey_data.columns:
            # Switching from a nested if to a guard statement + warning (MV).
            logger.warning(f"{column} not found in current dataset.")
            continue
            
        recoded_df[col] = df[col].apply(lambda x: str(int(x)) if isinstance(x, (int, float)) and x.is_integer() else str(x))
                recoded_df[col] = recoded_df[col].map(recode_info["mapping"])
        

        return survey_data


def aggregate(df, config, location_map, geographic_level, logger):
    """
    Aggregate the survey data to a given geography level provided the 
    rules given in the indicator_map.

    You can provide any 'geographic_level' that appears as a column on 
    the 'recoded' dataframe.
    """
    if geographic_level in location_map:
        location_mapping = location_map[geographic_level]
    else:
        logger.warning(f"Geographic level {geographic_level} not found in location_map.")
        location_mapping = {}

    results = []
    for indicator_id, indicator_info in config["indicators"].items():
        for question_id, question_info in indicator_info["questions"].items():
            question_col = question_info["column"]
            question_id_config = question_info["question_id"]
            options = question_info["options"]["values"]
            
            if question_col not in recoded_df.columns:
                logger.warning(f"'{question_col}' doesn't appear in the recoded file.")
                continue

            # convert to ints
            recoded_df[question_col] = pd.to_numeric(recoded_df[question_col], errors='coerce')
            recoded_df[question_col] = recoded_df[question_col].fillna(0).astype(int)
            
            try:
                grouped = recoded_df.groupby(geographic_level)[question_col]
            except KeyError as e:
                raise KeyError(f"Invalid geography level: '{geographic_level}'!")

            universe = grouped.count()

            for option_id, option_value_list in question_info["options"].items():
                for option_value in option_value_list:
                    count = grouped.apply(lambda x: sum(x.isin([option_value])))
                    percentage = (count / universe * 100).fillna(0)

                    results.extend([
                        {
                            "indicator_id": indicator_id,
                            "survey_question_id": question_id_config,
                            "survey_question_option_id": option_value,
                            "location": location,
                            "count": c,
                            "universe": u,
                            "percentage": p,
                        }
                        for location, c, u, p in zip(universe.index, count, universe, percentage)
                    ])
                    
        # Indicator Level Aggregation
        indicator_cols = [q_info["column"] for q_info in indicator_info["questions"].values()]
        indicator_options = [q_info["options"]["values"] for q_info in indicator_info["questions"].values()]

        def indicator_check(row):
            for col, opts in zip(indicator_cols, indicator_options):
                if row[col] not in opts:
                    return 0
            return 1

        recoded_df[indicator_id] = recoded_df.apply(indicator_check, axis=1)

        indicator_grouped = recoded_df.groupby(geographic_level)[indicator_id]
        indicator_count = indicator_grouped.sum()
        indicator_universe = indicator_grouped.count()
        indicator_percentage = (indicator_count / indicator_universe * 100).fillna(0)

        results.extend([
            {
                "indicator_id": indicator_id,
                "survey_question_id": "",
                "survey_question_option_id": "",
                "location": location,
                "count": c,
                "universe": u,
                "percentage": p,
            }
            for location, c, u, p in zip(indicator_universe.index, indicator_count, indicator_universe, indicator_percentage)
        ])

    return pd.DataFrame(results)


# Transforms all indicator and question data.
def transform_data(logger):
    logger.info("Transforming data...")

    config = configparser.ConfigParser()
    config.read(WORKING_DIR / "conf" / ".conf")

    with open(WORKING_DIR / "indicator_map.json", "r") as f:
        indicator_map = json.load(f)

    with open(WORKING_DIR / "location_map.json", "r") as f:
        location_map = json.load(f)

    # Raw Survey Data
    df = pd.read_csv(config["survey_responses"] , encoding="latin-1")

    # Geocode with zones
    geo = pd.read_csv(config["geocoded_info"])

    # combining district/zones
    df = df.merge(geo, left_on="Response ID", right_on="id")
    
    # Recode the data to match the ids supplied by Brian
    recoded = recode(df, indicator_map, logger)
    
    # Aggregate to the various levels
    citywide = aggregate(recoded, indicator_map, location_map, "city", logger)
    council_districts = aggregate(recoded, indicator_map, location_map, "district_number", logger)
    neighborhood_zones = aggregate(recoded, indicator_map, location_map, "zone_id", logger)

    # transformed_data_district.to_csv("district_test.csv", index=False)
    # Combine final dataframe
    df = pd.concat([citywide, council_district, neighborhood_zones], ignore_index=True)
    df["survey_id"] = 1
    df["year"] = '2024'

    df.to_csv(WORKING_DIR / "output" / "nvi_survey_2024.csv", index=False)
