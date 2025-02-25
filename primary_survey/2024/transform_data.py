import pandas as pd

# Adds a row to the output dataframe for a percentage value.
def add_percentage_output(output, count, universe, indicator_id, question_id, question_option_id):
    row = {
        "year": 2024,
        "count": count,
        "universe": universe,
        "percentage": count / universe * 100,
        "rate": None,
        "rate_per": None,
        "dollars": None,
        "indicator_id": indicator_id,
        "location_id": None,
        "survey_id": 1,
        "survey_question_id": question_id,
        "survey_question_option_id": question_option_id
    }
    output.loc[len(output)] = row

# Transforms indicator 1.1 and its associated questions.
def transform_indicator_1_1(df, output):
    #FIXME: sample data file does not have location information - will need this and grouping logic to calculate values by location

    # indicator 1.1
    add_percentage_output(
        output, 
        len(df[(df['Block_Neighborhood_Community_Group_Participation:Activity_Participation_Frequency'].isin([3, 4])) | (df['Community_Group_Outside_of_Neighborhod_Participation:Activity_Participation_Frequency'].isin([3, 4]))]), 
        len(df), 
        1.1, 
        None, 
        None
    )

    #TODO: consider creating a config file for the indicator questions and options to reduce the redundancy in the code

    # indicator 1.1, question 1
    add_percentage_output(
        output, 
        len(df[df['Block_Neighborhood_Community_Group_Participation:Activity_Participation_Frequency'] == 4]), 
        len(df), 
        1.1, 
        1, 
        4
    )
    add_percentage_output(
        output, 
        len(df[df['Block_Neighborhood_Community_Group_Participation:Activity_Participation_Frequency'] == 3]), 
        len(df), 
        1.1, 
        1, 
        3
    )
    add_percentage_output(
        output, 
        len(df[df['Block_Neighborhood_Community_Group_Participation:Activity_Participation_Frequency'] == 2]), 
        len(df), 
        1.1, 
        1, 
        2
    )
    add_percentage_output(
        output, 
        len(df[df['Block_Neighborhood_Community_Group_Participation:Activity_Participation_Frequency'] == 1]), 
        len(df), 
        1.1, 
        1, 
        1
    )

    # indicator 1.1, question 2
    add_percentage_output(
        output, 
        len(df[df['Community_Group_Outside_of_Neighborhod_Participation:Activity_Participation_Frequency'] == 4]), 
        len(df), 
        1.1, 
        2, 
        4
    )
    add_percentage_output(
        output, 
        len(df[df['Community_Group_Outside_of_Neighborhod_Participation:Activity_Participation_Frequency'] == 3]), 
        len(df), 
        1.1, 
        2, 
        3
    )
    add_percentage_output(
        output, 
        len(df[df['Community_Group_Outside_of_Neighborhod_Participation:Activity_Participation_Frequency'] == 2]), 
        len(df), 
        1.1, 
        2, 
        2
    )
    add_percentage_output(
        output, 
        len(df[df['Community_Group_Outside_of_Neighborhod_Participation:Activity_Participation_Frequency'] == 1]), 
        len(df), 
        1.1, 
        2, 
        1
    )

# Transforms all indicator and question data.
def transform_data():
    print("Transforming data...")

    data = pd.read_csv("./input/data.csv")
    output = pd.DataFrame(columns=["year", "count", "universe", "percentage", "rate", "rate_per", "dollars", "indicator_id", "location_id", "survey_id", "survey_question_id", "survey_question_option_id"])

    transform_indicator_1_1(data, output)

    print(output)

    print("Data transformed successfully!")
