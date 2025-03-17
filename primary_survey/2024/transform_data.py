import pandas as pd


def recode_aggregate(survey_data, config_file, geography):

    with open(config_file, "r") as f:
        config = json .load(f)

    df = survey_data

    # Columns that need to be combined
    # for combined_col, cols_to_combine in config['combine_columns'].items():
    #     df[combined_col] = df[cols_to_combine].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)
    #     df.drop(columns=cols_to_combine, inplace = True)

    # Recoding
    for column, recode_info in config['recode'].items():
        if column in df.columns:
            if recode_info['type'] == 'categorical':
                mapping = {str(k): v for k, v in recode_info['mapping'].items()}
                df[column] = df[column].astype(str).map(mapping).fillna(df[column])
        elif recode_info['type'] == 'numeric':
            try:
                df[column] = pd.to_numeric(df[column], errors='coerce')
            except:
                print(f"Column {column} could not be converted to numeric!")

    results = []

    for indicator_id, indicator_info in config['indicators'].items():
        for question_id, question_info in indicator_info['questions'].items():
            question_col = question_info['column']
            if question_col in df.columns:
                if geography == 'city':
                    grouped = df.groupby('city')[question_col]
                elif geography == 'district_level':
                    grouped = df.groupby('district_number')[question_col]
                elif geography == 'zone':
                    grouped = df.groupby('zone_id')[question_col]
                else:
                    raise ValueError("Invalid geography level!")
                
                universe = grouped.count()
                
                for option_id, option_value in question_info['options'].items():
                    if isinstance(option_value, list):
                        count = grouped.apply(lambda x: sum(x.isin(option_value)))
                    else:
                        count = grouped.apply(lambda x: sum(x == option_value))

                    percentage = (count / universe * 100).fillna(0)

                    for location, c, u, p in zip(universe.index, count, universe, percentage):
                        results.append({
                            'indicator_id': indicator_id,
                            'survey_question_id': question_id,
                            'survey_question_option_id': option_id,
                            'location': location,
                            'count': c,
                            'universe': u,
                            'percentage': p
                        })

    return pd.DataFrame(results)

# Transforms all indicator and question data.
def transform_data():
    print("Transforming data...")

    # Raw Survey Data 
    os.chdir('Q:/NVI/2024/Raw Survey Response')
    df = pd.read_csv('final_nvi_surveys_complete_20250224.csv', encoding='latin-1') 

    # Geocode with zones
    os.chdir('Q:/NVI/2024/Prelimenary Data')
    geo = pd.read_csv('nvi_geocode_tempzones_20250311.csv') 

    # combining district/zones 
    df = df.merge(geo, left_on='Response ID', right_on='id')

    df = df[['Response ID', 
               'Stay_12_Months:Agreement_Statements_Access_and_Support',
               'Block_Neighborhood_Community_Group_Participation:Activity_Participation_Frequency',
               'Community_Group_Outside_of_Neighborhod_Participation:Activity_Participation_Frequency',
               'zone_id', 'district_number']].copy()

    config = "indicator_map.json"

    transformed_data_district = recode_aggregate(df, config, 'district_level')
    transformed_data_zone = recode_aggregate(df, config, 'zone')

    transformed_data_district.to_csv('district_test.csv')
