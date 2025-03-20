import os
import json
import pandas as pd


def recode(survey_data, config, logger):
    # Columns that need to be combined
    # for combined_col, cols_to_combine in config['combine_columns'].items():
    #     survey_data[combined_col] = survey_data[cols_to_combine].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)
    #     survey_data.drop(columns=cols_to_combine, inplace = True)

    # Recoding
    for column, recode_info in config["recode"].items():
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


def aggregate(recoded, config, logger):

    results = []
    for indicator_id, indicator_info in config["indicators"].items():
        for question_id, question_info in indicator_info["questions"].items():
            question_col = question_info["column"]
            if question_col in recoded.columns:
                if geography == "city":
                    grouped = recoded.groupby("city")[question_col]
                elif geography == "district_level":
                    grouped = recoded.groupby("district_number")[question_col]
                elif geography == "zone":
                    grouped = recoded.groupby("zone_id")[question_col]
                else:
                    raise ValueError("Invalid geography level!")

                universe = grouped.count()

                for option_id, option_value in question_info["options"].items():
                    if isinstance(option_value, list):
                        count = grouped.apply(
                            lambda x: sum(x.isin(option_value))
                        )
                    else:
                        count = grouped.apply(lambda x: sum(x == option_value))

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


    with open("indicator_map.json", "r") as f:
        config = json.load(f)


    # Raw Survey Data
    os.chdir("Q:/NVI/2024/Raw Survey Response")
    df = pd.read_csv(
        "final_nvi_surveys_complete_20250224.csv", encoding="latin-1"
    )

    # Geocode with zones
    os.chdir("Q:/NVI/2024/Prelimenary Data")
    geo = pd.read_csv("nvi_geocode_tempzones_20250311.csv")

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
    recoded = recode(df, config, logger)
    
    # Aggregate to the various levels
    citywide = ...
    council_districts = ...
    neighborhood_zones = ...

    # transformed_data_district.to_csv("district_test.csv", index=False)
