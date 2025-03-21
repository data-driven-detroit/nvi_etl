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
    for column, recode_info in indicator_map["recode"].items():
        if column not in survey_data.columns:
            # Switching from a nested if to a guard statement + warning (MV).
            logger.warning(f"{column} not found in current dataset.")
            continue

        if recode_info["type"] == "categorical":
            # I'm skipping the type conversion on the mapping because
            # they all look correct in the indicator_map (MV).

            survey_data[column] = (
                survey_data[column]
                .astype(str)
                .map(recode_info["mapping"])
                .fillna(survey_data[column])
            )

        elif recode_info["type"] == "numeric":
            try:
                survey_data[column] = pd.to_numeric(survey_data[column], errors="coerce")

            except ValueError:  # FIXME Could this throw other errors?
                logger.error(
                    f"Column {column} could not be converted to numeric!"
                )

        return survey_data


def aggregate(recoded, indicator_map, geographic_level, logger):
    """
    Aggregate the survey data to a given geography level provided the 
    rules given in the indicator_map.

    You can provide any 'geographic_level' that appears as a column on 
    the 'recoded' dataframe.
    """

    results = []
    for indicator_id, indicator_info in indicator_map["indicators"].items():
        for question_id, question_info in indicator_info["questions"].items():
            question_col = question_info["column"]
            if question_col not in recoded.columns:
                logger.warning(f"'{question_col}' doesn't appear in the recoded file.")
                continue
            
            try:
                grouped = recoded.groupby(geographic_level)[question_col]
            except KeyError as e:
                raise KeyError(f"Invalid geography level: '{geographic_level}'!")

            universe = grouped.count()

            for option_id, option_value in question_info["options"].items():
                # I changed everything in 'indicator_map.json' to be lists to skip the
                # type check here (MV)
                count = grouped.apply(lambda x: sum(x.isin(option_value)))

                # TODO: Ask Brian if percentages should be 100 scaled or 0-1
                percentage = (count / universe * 100).fillna(0)

                for location, c, u, p in zip(
                    universe.index, count, universe, percentage
                ):
                    results.append(
                        {
                            "indicator_id": indicator_id,
                            "survey_question_id": question_id,
                            "survey_question_option_id": option_id,
                            "location": location,
                            "count": c,
                            "universe": u,
                            "percentage": p,
                        }
                    )

    return pd.DataFrame(results)


# Transforms all indicator and question data.
def transform_data(logger):
    logger.info("Transforming data...")

    config = configparser.ConfigParser()
    config.read(WORKING_DIR / "conf" / ".conf")

    with open(WORKING_DIR / "indicator_map.json", "r") as f:
        indicator_map = json.load(f)

    # Raw Survey Data
    df = pd.read_csv(config["survey_responses"] , encoding="latin-1")

    # Geocode with zones
    geo = pd.read_csv(config["geocoded_info"])

    # combining district/zones
    df = df.merge(geo, left_on="Response ID", right_on="id")

    df = df[
        [
            "Response ID",
            "Stay_12_Months:Agreement_Statements_Access_and_Support",
            "Block_Neighborhood_Community_Group_Participation:Activity_Participation_Frequency",
            "Community_Group_Outside_of_Neighborhod_Participation:Activity_Participation_Frequency",
            "zone_id",
            "district_number",
        ]
    ].copy()
    
    # Recode the data to match the ids supplied by Brian
    recoded = recode(df, indicator_map, logger)
    
    # Aggregate to the various levels
    citywide = ...
    council_districts = ...
    neighborhood_zones = ...

    # transformed_data_district.to_csv("district_test.csv", index=False)
