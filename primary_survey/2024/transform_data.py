import json
import pandas as pd
import numpy as np


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


def rename_columns(df):
    return df.rename(
        columns={
            'Block_Neighborhood_Community_Group_Participation:Activity_Participation_Frequency': 'NeighborhoodParticipation',
               'Community_Group_Outside_of_Neighborhod_Participation:Activity_Participation_Frequency': 'OutNeighborParticipation',
               'Community development organization(s):Community_Engagement_Process_Participation':'CDO_Participant', 
               'City government:Community_Engagement_Process_Participation':'CityGov_Participant', 
               'I have not participated in a community engagement process:Community_Engagement_Process_Participation':'Non_CityGov_Participant', 
               'Other (please describe)::Community_Engagement_Process_Participation':'Other_Participant',
               'Last12Months_PurchasedLot':'PurchasedLot',
               'Last12Months_CleanLotOwned':'CleanOwnLot',
               'Last12Months_CleanLotNotOwned':'CleanOtherLot',
               'Last12Months_ImprovedVacHousing':'SecureVacancy',
               'Last12Months_ImprovedAlleys':'CleanAlley',
               'Last12Months_ImprovedParks':'CleanPark', 
               'I have not participated in any of these activities:In_The_Last_12_Months':'residentagency_none',
               'Housing:Agreement_Statements_Detroit_Access':'HousingAccess', 
               'Financial:Agreement_Statements_Detroit_Access': 'FinanceAccess', 
               'Medical_Care:Agreement_Statements_Detroit_Access':'MedicalAccess', 
               'Mental_Health_Services:Agreement_Statements_Detroit_Access':'MentalHealthAccess', 
               'Human_Services:Agreement_Statements_Detroit_Access':'HumanServiceAccess', 
               'Currently_Employed':'EmploymentStatus', 
               'Detroit_Business_Ownership':'OwnBusiness', 
               'Savings_Account':'SavingAccount', 
               'Safe_Reliable_Transportation_Access_Frequency':'SafeTransport', 
               'Detroit:Residence_Length':'CityLength', 
               'Current_Neighborhood:Residence_Length':'NeighborLength', 
               'Personal_Move_Due_To_COVID':'Move_Covid', 
               'Neighbors_Move_Due_To_COVID':'NeighborMove',
               'Quality_of_Housing:Neighborhood_Satisfaction':'Neighborhood_Quality', 
               'Condition_of_Vacant_Residential_Buildings:Neighborhood_Satisfaction':'Neighborhood_ResVacant', 
               'Condition_of_Vacant_Commercial_Buildings:Neighborhood_Satisfaction':'Neighborhood_ComVacant', 
               'Condition_of_Vacant_Lots:Neighborhood_Satisfaction':'Neighborhood_LotVacant',
               'Neighborhood_Infrastructure:Neighborhood_Satisfaction':'Neighborhood_Infra',  
               'Neighborhood:Neighborhood_Safety_Perception':'Neighborhood_Safe',
               'Physical:Health_Satisfaction':'Physical_Health', 
               'Mental_Health:Health_Satisfaction':'Mental_Health', 
               'SocioEmotional_Support:Health_Satisfaction':'SocioEmotional_Support', 
               'QualityLife_Satisfaction':'QualityLife', 
               'K_12_Public_Charter_Schools:Neighborhood_Access_Satisfaction':'Neighborhood_Schools',
               'Parks_Playgrounds_and_Public Spaces:Neighborhood_Access_Satisfaction':'Neighborhood_Parks',
               'Childcare:Neighborhood_Access_Satisfaction':'Neighborhood_Childcare',
               'Amenities:Neighborhood_Access_Satisfaction':'Neighborhood_Retail', 
               'Medical_Care:Neighborhood_Health_Related_Access':'Neighborhood_MedCare', 
               'Mental_Health_Services:Neighborhood_Health_Related_Access':'Neighborhood_MentalHealth', 
               'Places_to_be_Active:Neighborhood_Health_Related_Access':'Neighborhood_Active', 
               'Fresh_Healthy_Food:Neighborhood_Health_Related_Access':'Neighborhood_Food',
               'YouthProgram_Participation_question1':'Extracurric_under18Count', 
               'YouthProgram_Participation_question2':'Leadership_under18Count', 
               'Childcare_Prevent_Work_Meetings_Appointments':'Work_Childcare',
               'Neighborhood:Youth_Safety_Perception':'NeighborSafe_Under18',
               'Public_Spaces:Youth_Safety_Perception':'PublicSafe_Under18',
               'Stay_12_Months:Agreement_Statements_Access_and_Support':'Stay1Year',
               "Meet_Basic_Needs:Agreement_Statements_Access_and_Support":'MeetNeeds',
               'Child_Quality_Recreation:Agreement_Statements_Access_and_Support':'QualRecreation',
               'Fam_Incarcerated_YN:Crime_and_COVID_Best_Answer':'Incarceration',
               'Victim_Neighborhood_YN:Crime_and_COVID_Best_Answer':'CrimeVictim', 
               'Comfortable_Reporting_YN:Crime_and_COVID_Best_Answer':'ReportCrime',
               'COVID_HealthImpact_YN:Crime_and_COVID_Best_Answer':'CovidHealth', 
               'COVID_HousingImpact_YN:Crime_and_COVID_Best_Answer':'CovidHousing', 
               'Risk_LosingHome_YN:Crime_and_COVID_Best_Answer':'CovidLoss',
               'Bail_System:Negatively_Impacted_Or_Harmed_By':'Bail_Negative', 
               'Illegal_Dump:Neighborhood_Environmental_Severity': 'Illegal_Dump',
               'Air_Pollution:Neighborhood_Environmental_Severity': 'Air_Pollution',
               'Lead:Neighborhood_Environmental_Severity': 'Lead',
               'Stray_Dogs:Neighborhood_Environmental_Severity': 'Stray_Dogs',
               'Flooding:Neighborhood_Environmental_Severity': 'Flooding',
               'Water_Quality:Neighborhood_Environmental_Severity': 'Water_Quality',
               'Heat:Neighborhood_Environmental_Severity': 'Heat',
               'Power-Outages:Neighborhood_Environmental_Severity': 'Power-Outages',
               'Brownfields:Neighborhood_Environmental_Severity': 'Brownfields',
               "I don't have a computer/smartphone because it's too expensive:Technology_Statements":'Tech_ComExpensive', 
               "I don't have a computer/smartphone because I don't need to use the internet:Technology_Statements":'Tech_NoNeed', 
               "I don't understand/feel confident using the internet:Technology_Statements":'Tech_NotConfident',
               "I don't have internet access at home because it's too expensive:Technology_Statements":'Tech_InternetExpensive', 
               "I'm worried about my privacy/security:Technology_Statements":'Tech_Privacy',
               "I do not have any of these concerns:Concerns_Using_Devices":'Tech_NoIssue',
               'Employment_Impacted_By_COVID':'Covid_Unimpact',
               'Number_Of_Youth_In_Household':'YouthinHousehold',
               'Racial_Equity1': 'Race_Oppurtunity',
               'Racial_Equity2': 'Race_Work',
               'Racial_Equity3': 'Race_Health'
            }
        )


# Transforms all indicator and question data.
def transform_data():
    print("Transforming data...")

    data = pd.read_csv("./input/data.csv")
    output = pd.DataFrame(columns=["year", "count", "universe", "percentage", "rate", "rate_per", "dollars", "indicator_id", "location_id", "survey_id", "survey_question_id", "survey_question_option_id"])

    # transform_indicator_1_1(data, output)
    df = rename_columns(data)
    df_nhood = survey_to_zone(df)
    df_district = survey_to_district(df)

    df_nhood = transform_wide(df_nhood)
    df_district = transform_wide(df_district)

    output = pd.concat([output, df_nhood, df_district])

    print(output)

    print("Data transformed successfully!")



def stay1year_recode(df):
    return df


def recode_data(df):
    #stay1year_recode
    df['Stay1Year'] = df['Stay1Year'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['stay1year_recode'] = df['Stay1Year']

    # resparticipant 
    df['NeighborhoodParticipation'] = df['NeighborhoodParticipation'].replace([1, 2, 3, 4],[0, 0, 1, 1])
    df['OutNeighborParticipation'] = df['OutNeighborParticipation'].replace([1, 2, 3, 4],[0, 0, 1, 1])
    def respar(row):
        if row['NeighborhoodParticipation'] == 1:
            return 1
        elif row['OutNeighborParticipation'] == 1:
            return 1
        return 0
    df['resparticipant'] = df.apply(lambda row: respar(row), axis=1)

    # cityaccess -----
    # housingaccess_city
    df['HousingAccess'] = df['HousingAccess'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['housingaccess_city'] = df['HousingAccess']

    # financeaccess_city
    df['FinanceAccess'] = df['FinanceAccess'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['financeaccess_city'] = df['FinanceAccess']

    # medicalcaresupport_city
    df['MedicalAccess'] = df['MedicalAccess'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['medicalcaresupport_city'] = df['MedicalAccess']

    # mentalhealthsupport_city
    df['MentalHealthAccess'] = df['MentalHealthAccess'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['mentalhealthsupport_city'] = df['MentalHealthAccess']

    # humanservicessupport_city
    df['HumanServiceAccess'] = df['HumanServiceAccess'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['humanservicessupport_city'] = df['HumanServiceAccess']
        
    def _cityaccess_recode(row):
        if row['housingaccess_city'] and row['financeaccess_city'] and row['medicalcaresupport_city'] and row['mentalhealthsupport_city'] and row['humanservicessupport_city']:
            return 1
        else:
            return 0
    df['cityaccess'] = df.apply(lambda row: _cityaccess_recode(row), axis=1)

    # commengage
    def com(row):
        if row['Non_CityGov_Participant'] == 3:
            return 0
        else:
            return 1
    df['commengage'] = df.apply(lambda row: com(row), axis=1)

    # residentagency
    df["residentagency"] = np.where((df['residentagency_none'] == 0), 0, 1)

    # FIX THIS TO NEW INDICATOR ###########################################################################################
    # anybusiness 
    df['anyBusiness'] = df['OwnBusiness'].replace([1, 2, 3, 4, 5],[1, 1, 0, 0, 0])
    # df['anybusiness'] = df['OwnBusiness']
    # building_business
    df['BuildingBusiness'] = df['OwnBusiness'].replace([1, 2, 3, 4, 5],[0, 1, 0, 0, 0])
    df['BuildingBusiness'] = df['BuildingBusiness']

    #savingaccount_recode
    df['SavingAccount'] = df['SavingAccount'].replace([1, 2],[1, 0])
    df['savingaccount_recode'] = df['SavingAccount']

    # detroit_work
    df['EmploymentStatus'] = df['EmploymentStatus'].replace([1, 2, 3, 4],[1, 0, 1, 0])
    df['detroit_work'] = df['EmploymentStatus']

    ########## Add health finance indicator 

    #safetransport_recode
    df['SafeTransport'] = df['SafeTransport'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    df['safetransport_recode'] = df['SafeTransport']

    #resturnover
    df['CityLength'] = df['CityLength'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    df['resturnover'] = df['CityLength']

    #housingquality
    df['Neighborhood_Quality'] = df['Neighborhood_Quality'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['housingquality'] = df['Neighborhood_Quality']

    #resvacancy
    df['Neighborhood_ResVacant'] = df['Neighborhood_ResVacant'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['resvacancy'] = df['Neighborhood_ResVacant']

    #vacantlot
    df['Neighborhood_LotVacant'] = df['Neighborhood_LotVacant'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['vacantlot'] = df['Neighborhood_LotVacant']

    #infrastructure
    df['Neighborhood_Infra'] = df['Neighborhood_Infra'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['infrastructure'] = df['Neighborhood_Infra']

    ###################### add commercial vacancy indicator 

    # ressafety
    df['Neighborhood_Safe'] = df['Neighborhood_Safe'].replace([0, 1, 2, 3, 4],[0, 0, 0, 1, 1])
    df['ressafety'] = df['Neighborhood_Safe']

    #health access
    #medcare
    df['Neighborhood_MedCare'] = df['Neighborhood_MedCare'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['medcare'] = df['Neighborhood_MedCare']

    #mentalhealthcare
    df['Neighborhood_MentalHealth'] = df['Neighborhood_MentalHealth'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['mentalhealthcare'] = df['Neighborhood_MentalHealth']

    #activespaces
    df['Neighborhood_Active'] = df['Neighborhood_Active'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['activespaces'] = df['Neighborhood_Active']

    #healthyfood
    df['Neighborhood_Food'] = df['Neighborhood_Food'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['healthyfood'] = df['Neighborhood_Food']

    def _healthaccess_recode(row):
        if row['medcare'] and row['mentalhealthcare'] and row['activespaces'] and row['healthyfood']:
            return 1
        else:
            return 0
    df['healthaccess'] = df.apply(lambda row: _healthaccess_recode(row), axis=1)

    #schoolaccess
    df['Neighborhood_Schools'] = df['Neighborhood_Schools'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['schoolaccess'] = df['Neighborhood_Schools']

    #parkaccess
    df['Neighborhood_Parks'] = df['Neighborhood_Parks'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['parkaccess'] = df['Neighborhood_Parks']

    #retailaccess
    df['Neighborhood_Retail'] = df['Neighborhood_Retail'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 0, 1, 1])
    df['retailaccess'] = df['Neighborhood_Retail']

    # reshealth
    df['Physical_Health'] = df['Physical_Health'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    df['Mental_Health'] = df['Mental_Health'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    def _reshealth_recode(row):
        if row['Physical_Health'] and row['Mental_Health']:
            return 1
        else:
            return 0 
    df['reshealth'] = df.apply(lambda row: _reshealth_recode(row), axis=1)

    # ressuport
    df['SocioEmotional_Support'] = df['SocioEmotional_Support'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    df['ressupport'] = df['SocioEmotional_Support']

    # lifequality
    df['QualityLife'] = df['QualityLife'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    df['lifequality'] = df['QualityLife']

################ Add public safety indicator
################ Add housing needs
################ Add future needs
################ Add billsability
################ Add adequate food
################ Add housingquality


    #incarceration_recode
    df['Incarceration'] = df['Incarceration'].replace([0, 1, 2],[0, 1, 0])
    df['incarceration_recode'] = df['Incarceration']


    #envirohealth
    def _environ_recode(row):
        if row['Illegal_Dump'] == 4 and row['Air_Pollution'] == 4 and row['Lead'] == 4 and row['Stray_Dogs'] == 4 and row['Flooding'] == 4 and row['Water_Quality'] == 4 and row['Heat'] == 4 and row['Power-Outages'] == 4 and row['Brownfields'] == 4:
            return 1
        else:
            return 0
    df['envirohealth'] = df.apply(lambda row: _environ_recode(row), axis=1)

    #covidjobloss
    df['covidjobloss'] = df['Covid_Unimpact'].replace([0, 1, 2, 3, 4, 5],[0, 0, 0, 1, 1, 1])

    # oppurtunity
    df['Race_Oppurtunity'] = df['Race_Oppurtunity'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    df['oppurtunity'] = df['Race_Oppurtunity']

    # work_discrimination
    df['Race_Work'] = df['Race_Work'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    df['work_discrimination'] = df['Race_Work']

    # health_discrimination
    df['Race_Health'] = df['Race_Health'].replace([1, 2, 3, 4, 5],[0, 0, 0, 1, 1])
    df['health_discrimination'] = df['Race_Health']

################  Add internet

    #bail_recode
    df['Bail_Negative'] = df['Bail_Negative'].replace([0, 1, 2],[0, 1, 0])
    df['bail_recode'] = df['Bail_Negative']


################ Add crime report 
# #reportcrime_recode
# df['ReportCrime'] = df['ReportCrime'].replace([0, 1, 2],[0, 1, 0])
# df['reportcrime_recode'] = df['ReportCrime']

################ Add crimevictim
#crimevictim_Recode
# df['CrimeVictim'] = df['CrimeVictim'].replace([0, 1, 2],[0, 1, 0])
# df['crimevictim_Recode'] = df['CrimeVictim']


################ Add internet access
    #techissue
    # Combine Tech_ComExpensive, Tech_NoNeed, Tech_NotConfident, Tech_InternetExpensive, Tech_Privacy
    df['techissue'] = np.where((df['Tech_NoIssue'] == 6), 1, 0)
################ Add tech issue

    #youthleader
    def _youth_leader(row):
        if row['YouthinHousehold'] > 0:
            if row['Leadership_under18Count'] == 1:
                return 1
            else:
                return 0
        else:
            return None 
    df['Leadership_under18Count'] = df.apply(lambda row: _youth_leader(row), axis=1)
    df['youthleader'] = df['Leadership_under18Count']

    #youthsafe_neigh
    def _youth_safe(row):
        if row['YouthinHousehold'] > 0:
            if row['NeighborSafe_Under18'] == 3 or row['NeighborSafe_Under18'] == 4:
                return 1
            else:
                return 0
        else:
            return None 
    df['NeighborSafe_Under18'] = df.apply(lambda row: _youth_safe(row), axis=1)
    df['youthsafe_neigh'] = df['NeighborSafe_Under18']

    #youthsafe_park
    def _youth_park(row):
        if row['YouthinHousehold'] > 0:
            if row['PublicSafe_Under18'] == 3 or row['PublicSafe_Under18'] == 4:
                return 1
            else:
                return 0
        else:
            return None 

    df['PublicSafe_Under18'] = df.apply(lambda row: _youth_park(row), axis=1)
    df['youthsafe_park'] = df['PublicSafe_Under18']

    #youth_access
    def _youth_rec(row):
        if row['YouthinHousehold'] == 1:
            if row['QualRecreation'] == 4 or row['QualRecreation'] == 5:
                return 1
            else:
                return 0
        else:
            return None 
    df['QualRecreation'] = df.apply(lambda row: _youth_rec(row), axis=1)
    df['youth_access'] = df['QualRecreation']


    #youthrec
    def _youth_rec(row):
        if row['YouthinHousehold'] > 0:
            if row['Extracurric_under18Count'] == 1:
                return 1
            else:
                return 0
        else:
            return None 

    df['Extracurric_under18Count'] = df.apply(lambda row: _youth_rec(row), axis=1)
    df['youthrec'] = df['Extracurric_under18Count']

    
    # childcare_access
    df['Work_Childcare'] = df['Work_Childcare'].replace([0, 1, 2, 3, 4, 5, 'Never', 'Rarely', 'Sometimes', 'Often', 'Always'],[0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1])
    def _childcare_recode(row):
        if row['Work_Childcare'] == 1:
            return 1
        else:
            return 0 
    df['childcare_access'] = df.apply(lambda row: _childcare_recode(row), axis=1)


# TODO: finish adding survey fields here:
    nvi_survey_fields = df[['zonebecdd', 'detroit_city', 'stay1year_recode', 'resparticipant', 'cityaccess', 'commengage','residentagency', 'savingaccount_recode', 
                            'safetransport_recode', 'resturnover', 'resvacancy', 'vacantlot', 'infrastructure', 'housingquality', 'healthaccess', 
                            'schoolaccess', 'parkaccess', 'retailaccess', 'incarceration_recode', 'bail_recode', 'envirohealth', 'techissue', 
                            'covidjobloss', 'youthleader', 'youthsafe_neigh', 'youthsafe_park', 'youth_access', 'youthrec']].copy()
    
    return nvi_survey_fields


# TODO: 3/12/2024 Move aggregation code to sum to district and nhood_zone here!
def survey_to_zone(df):
    return



def survey_to_district(df):
    return
# make map for location ids to geo columns for district and nhoodzone 

# TODO: redo for question option rows 
def transform_wide(df, config_file):
    with open(config_file, "r") as f:
        config = json.load(f)

    geographies = df.index if hasattr(df, 'index') and not isinstance(df.index, pd.RangeIndex) else df.index.tolist()

    for geo in geographies:
        for indicator_name, indicator_info in config.items():
            count_col = f"{indicator_name}_count"
            total_col = f"{indicator_name}_total"
            percent_col = f"{indicator_name}_percent"

            if count_col in df.columns and total_col in df.columns and percent_col in df.columns:
                indicator_data = {
                    "id": indicator_info['id'],
                    "survey_question_id": indicator_info['survey_question_id'],
                    "survey_question_option_id": indicator_info['survey_question_id'],
                    "location_id": geo,
                    "count": df.loc[geo, count_col] if isinstance(df.index, pd.MultiIndex) else df.loc[geo, count_col],
                    "total": df.loc[geo, total_col] if isinstance(df.index, pd.MultiIndex) else df.loc[geo, total_col],
                    "percentage": df.loc[geo, percent_col] if isinstance(df.index, pd.MultiIndex) else df.loc[geo, percent_col],
                }
                transform_data.append(indicator_data)

    return pd.DataFrame(transform_data)

