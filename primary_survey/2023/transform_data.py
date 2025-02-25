import pandas as pd

def transform_data():
    print("Transforming data...")

    variable_indicators = pd.read_csv("./conf/variable_indicators.csv")
    data = pd.read_excel("./input/data.xlsx")

    # remove rows with no data
    data.dropna(subset=["district"], inplace=True)

    # remove the data columns that are not in the variable list
    data_columns = data.columns
    for column in data_columns:
        if column != "district" and column not in variable_indicators["variable_name"].values:
            data.drop(column, axis=1, inplace=True)

    # pivot the data wide to long
    data = pd.melt(data, id_vars=["district"], var_name="variable", value_name="value")

    # join the data with the variable_list
    data = pd.merge(data, variable_indicators, left_on="variable", right_on="variable_name", how="left")

    # add type column
    data["type"] = data["variable"].apply(lambda x: "count" if x.startswith("num_") else "universe" if x.startswith("total_") else "percentage")

    # pivot data so each row has district, indicator_id, and the value for each type
    data = data.pivot_table(index=["district", "indicator_id"], columns="type", values="value", aggfunc="first").reset_index()

    # add survey_id and year
    data["survey_id"] = 1
    data["year"] = 2023

    # rename district to location_id
    data.rename(columns={"district": "location_id"}, inplace=True)
    
    # add blank columns not applicable to this data
    data["rate"] = None
    data["rate_per"] = None
    data["dollars"] = None
    data["survey_question_id"] = None
    data["survey_question_option_id"] = None

    # re-order the columns to match the database table
    data = data[["year", "count", "universe", "percentage", "rate", "rate_per", "dollars", "indicator_id", "location_id", "survey_id", "survey_question_id", "survey_question_option_id"]]
    data.to_csv("./output/data_transformed.csv", index=False)

    print("Data transformed successfully!")
