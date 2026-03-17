from datetime import date
import os
from pathlib import Path
import tomllib
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from nvi_etl.geo_reference import (
    pull_council_districts, 
    pull_zones, 
    pull_cdo_boundaries,
)


WORKING_DIR = Path(__file__).parent # v2025
BASE_DIR = Path(__file__).parent.parent.parent
SURVEY_YEAR = 2025

OUTPUT_COLUMN_ORDER = [
    "location",
    "location_id",
    "summary_level",
    "full_column",
    "response_type",
    "db_question_code",
    "indicator_db_id",
    "topic_text",
    "question_text",
    "indicator_include",
    "universe_include",
    "db_answer_code", # filter_option_id
    "answer",
    "count",
    "universe",
    "percentage",
]

VALUE_COLUMNS = [
    # "id",
    "year",
    "count",
    "universe",
    "percentage",
    "rate",
    "rate_per",
    "dollars",
    "indicator_id",
    "location_id",
    "survey_id",
    "survey_question_id",
    "survey_question_option_id",
    "value_type_id",
    "index",
]

today = date.today().strftime("%Y%m%d")

with open(BASE_DIR / "config.toml", "rb") as f:
    config = tomllib.load(f)


def get_engine(db_name=None):
    user = config["db"]["user"]
    password=config["db"]["password"]
    database=db_name or config["db"]["database"]
    host=config["db"]["host"]

    return create_engine(f"postgresql+psycopg://{user}:{password}@{host}:5432/{database}")


def combine_survey_and_geocoded(frame, geoframe):
    return gpd.GeoDataFrame(
        frame.merge(
            geoframe[["response_id", "geometry"]], 
            on="response_id"
        )
    )


def add_districts_and_zones(geocoded, districts, zones):
    return (
        geocoded
        .to_crs(2898)
        .sjoin(districts[["geometry", "district_number"]], how="left", predicate="within")
        .drop(columns="index_right")
        .sjoin(zones[["geometry", "zone_id"]], how="left", predicate="within")
        .assign(citywide='citywide') # This will be true of all items
    )


def base_counts(frame: pd.DataFrame) -> pd.DataFrame:
    tables = []
    for summary in ("citywide", "district_number", "zone_id"):
        tables.append(
            frame
            .groupby(summary, dropna=False)
            .size()
            .to_frame()
            .reset_index()
            .rename(columns={summary: "location"})
            .fillna({"location": "[NO ADDRESS PROVIDED]"})
        )

    # Drop duplicates removes second [NO ADDRESS PROVIDED]
    # reset freshens in index to be 0 to n, no repeats
    return (
        pd.concat(tables)
        .drop_duplicates(subset="location", keep="last")        
        .reset_index(drop=True)
    )


def append_universe_and_percentages(table):
    included = table["universe_include"]

    # Calculate a percentage column
    total = (
        table[included]
        .groupby("location")["count"]
        .transform("sum")
    )

    table["universe"] = total
    table.loc[included, "percentage"] = table.loc[included, "count"] / total

    return table


def aggregate_question(frame: pd.DataFrame, column_name: str, 
    summary: str, labels: pd.DataFrame) -> pd.DataFrame:

    """
    This assumes question is answered with a selection from a likert item

    in:
        frame: our current dataframe
        column_name: the name of the frame we're aggregating
        summary: the summary level we're working with -- another column
        labels: rows of the datadictionary associated with the column name

    out:
        The rows aggregated
    """
    table = (
        frame[[column_name, summary]]
        .value_counts(dropna=False)
        .reset_index(summary)
    )

    # Reference for filling in join misses
    reference = labels.iloc[0]

    table.index = table.index.astype(pd.Int64Dtype())

    table = (
        table
        .join(labels, how="outer")
        .sort_values([summary, column_name])
        .reset_index(drop=True)
        .rename(columns={summary: "location"})
        .fillna({
            "topic_text": reference["topic_text"],
            "question_text": reference["question_text"],
            "full_column": reference["full_column"],
            "answer": "[SKIPPED]",
            "db_question_code": reference["db_question_code"],
            "indicator_include": False,
            "universe_include": False,
            "indicator_db_id": reference["indicator_db_id"],
            "location": "[NO ADDRESS PROVIDED]",
        })
        .astype({
            "db_question_code": pd.Int64Dtype(), 
            "db_answer_code": pd.Int64Dtype(),
            "indicator_db_id": pd.Int64Dtype(),
            "universe_include": bool,
            "indicator_include": bool,
        })
        .assign(summary_level=summary)
    )

    return append_universe_and_percentages(table)


def roll_up_single(frame: pd.DataFrame, groupdictionary: pd.DataFrame, 
    survey_date: pd.Timestamp) -> tuple[pd.DataFrame, list[tuple[str,str]]]:

    # If the group is not a multi-select, we only need to deduplicate to 
    # get to 'full_column' level. 'full_column' are the values that match 
    # what is in the survey file.

    result = []
    errors = []
    for _, column in groupdictionary.drop_duplicates(subset="full_column").iterrows():

        # Skip columns that are not currently active
        if (
            (column["start_date"] >= survey_date)
            or (column["end_date"] < survey_date)
            or (not column["tabulate"])
        ):
            continue

        # Filter to the rows of the data dictionary relevant to the column of the
        # frame that we're aggregating.
        column_name = column["full_column"]

        labels = groupdictionary[
            groupdictionary["full_column"] == column_name
        ][[
            "survey_question_topic_id",
            "topic_text",
            "question_text",
            "survey_code",
            "answer",
            "full_column",
            "response_type",
            "db_question_code", 
            "db_answer_code",
            "indicator_include",
            "universe_include",
            "indicator_db_id",
        ]].set_index("survey_code")


        # This is a 'look-before-you-leap', but I want to track errors proactively
        if column_name not in frame.columns:
            errors.append((column_name, "name_mismatch"))
            continue

        # Remeber citywide is all ones!
        for summary in ("citywide", "district_number", "zone_id"):
            try:
                out = aggregate_question(frame, column_name, summary, labels)
                result.append(out)

            # This comes up if the column is str dtype, or other non-int things
            except ValueError:
                errors.append((column_name, "aggregation_error"))

    # concat doesn't like empty dataframes
    return (
        pd.concat(result) if result else pd.DataFrame(), 
        errors
    )


def aggregate_multiselect(frame: pd.DataFrame, groupdatadictionary: pd.DataFrame, 
    summary: str, survey_date:pd.Timestamp) -> pd.DataFrame:
    groups = frame.groupby(summary)
    universe = groups.size().rename("universe").reset_index() # all rows with value regardless of value

    # Skip columns that are not currently active, this is inverted from above.
    labels = groupdatadictionary[
        (groupdatadictionary["start_date"] <= survey_date)
        & (groupdatadictionary["end_date"] > survey_date)
        & groupdatadictionary["tabulate"]
    ]

    return (
        groups[labels["full_column"]]
        .count()
        .melt(ignore_index=False, var_name="full_column")
        .reset_index()
        .merge(
            labels[[
                "full_column", "db_question_code", "db_answer_code", "and_or", 
                "answer", "end_date", "group", "indicator_db_id", 
                "indicator_include", "question_text", "response_type", 
                "site_category", "start_date", "suppress_value", "survey_code",
                "survey_question_topic_id", "tabulate", "topic_text",
                "universe_include", "universe_query"
            ]], 
            on="full_column"
        )
        .merge(universe, on=summary)
        .rename(columns={"value": "count", summary: "location"})
        .assign(
            percentage=lambda frame: (100 * frame["count"] / frame["universe"]).round(2),
            summary_level=summary,
        )
        .astype({
            "db_question_code": pd.Int64Dtype(),
            "db_answer_code": pd.Int64Dtype(),
        })
    )


def roll_up_multiselect(frame: pd.DataFrame, groupdictionary: pd.DataFrame, 
    survey_date: pd.Timestamp) -> tuple[pd.DataFrame, list[tuple[str,str]]]:
    result = []
    errors = []

    # TODO: refactor this to pull it from being so deep in the call stack
    # It makes it # impossible to change the summary level for cdos.
    for summary in ("citywide", "district_number", "zone_id"):
        try:
            # The survey_date gets passed down
            out = aggregate_multiselect(frame, groupdictionary, summary, survey_date)
            result.append(out)

        # This comes up if the column is str dtype, or other non-int things
        except ValueError:
            group = groupdictionary.iloc[0]["group"]
            errors.append((group, "aggregation_error"))
    

    # concat doesn't like empty dataframes
    return (
        pd.concat(result) if result else pd.DataFrame(), 
        errors
    )


def create_question_rows(frame: pd.DataFrame, datadictionary: pd.DataFrame, 
    survey_date: pd.Timestamp) -> tuple[pd.DataFrame, list[tuple[str,str]]]:

    # Awkward that this line says 'group' so many times, but it takes 
    # questions that are groups and handles them appropriately. Solving 
    # for the fact that some questions are grouped under a 'topic' and
    # others are grouped as a multi-select.

    groups = datadictionary.groupby("group")

    tables = []
    errors = []
    for g, group in groups:
        response_type = group.iloc[0]["response_type"]

        try:
            if response_type in {"YES-NO", "SINGLE", "GROUPED-SINGLE"}:
                table, error = roll_up_single(frame, group, survey_date)
                
            elif response_type == "MULTI-SELECT":
                table, error = roll_up_multiselect(frame, group, survey_date)

            else:
                raise ValueError(f"{response_type} not valid.")

        except ValueError:
            raise ValueError(f"Failing on group {g}.")

        tables.append(table)
        errors.extend(error)
    
    return pd.concat(tables), errors


def main():
    """
    Process NVI from a dataframe, geodataframe and a datadictionary file
    (nvi_answer_key_<date>.xlsx).
    """
    
    # -------------------------------------------------------------------------
    # LOAD required files and combine the shape and answers
    # -------------------------------------------------------------------------

    frame = pd.read_csv(
        "Q:\\3_Projects\\NVI\\2025\\nvi_survey_data_2025_20260226.csv", 
        low_memory=False
    ).rename(columns={"Response ID": "response_id"})

    geoframe = gpd.read_file(
        "Q:\\3_Projects\\NVI\\2025\\Final Shapefiles\\Final2025NVIDataset_cleaned_20260304.shp"
    ).rename(columns={"Response_I": "response_id"})

    datadictionary = pd.read_excel(WORKING_DIR / "conf" / "nvi_answer_key_20260316.xlsx")

    location_dictionary = pd.read_excel(
        WORKING_DIR / "conf" / "locations_20260312.xlsx", 
        dtype={
            "location": str,
            "universe_include": bool,
        }
    )

    geocoded = combine_survey_and_geocoded(frame, geoframe)


    # -------------------------------------------------------------------------
    # Attach the geographic boundaries we're aggregating to
    # -------------------------------------------------------------------------

    DISTRICTS_YEAR = 2026
    ZONES_YEAR = 2026

    # city = pull_city_boundary() we determinted that we include all approved rows
    districts = pull_council_districts(DISTRICTS_YEAR)
    zones = pull_zones(ZONES_YEAR)

    # This is THE frame we'll carry into the rest of the aggregations
    complete_frame = add_districts_and_zones(geocoded, districts, zones)
    assert len(frame) == len(complete_frame)

    # This should take a different code path since we're not insisting 
    # on deduplication
    # cdos = pull_cdo_boundaries()

    # -------------------------------------------------------------------------
    # Run through the aggregations, rename the locations, save the results
    # -------------------------------------------------------------------------

    survey_date = pd.Timestamp(year=SURVEY_YEAR, month=1, day=1)

    # We're looping through groups (required for multi-select) so we want to
    # make sure that that group & response_type columns agree.

    # Aggregate everything to the 'table' level, we'll transform next
    # also peel off and log errors
    table, errors = create_question_rows(complete_frame, datadictionary, survey_date)

    print(f"{len(errors)} rows not aggregated rows (0 is expected).")
    if errors:
        print(errors)

    # This is saved as a different sheet, summarizing 
    summary_counts = base_counts(complete_frame)
    summary_counts.to_csv(WORKING_DIR / f"summary_counts_{today}.csv")

    table = (
        table
        .merge(location_dictionary, on="location", how="left")
        .astype({"location_id": pd.Int64Dtype()})
    )[OUTPUT_COLUMN_ORDER]

    table.to_csv(WORKING_DIR / f"test_output_{today}.csv", index=False)

    database_table = (
        table.rename(
            columns={
                "db_question_code": "survey_question_id",
                "indicator_db_id": "indicator_id",
                "db_answer_code": "survey_question_option_id",
            }
        )
        .assign(
            year=2025,
            rate=pd.NA,
            rate_per=pd.NA,
            dollars=pd.NA,
            survey_id=1, # This needs to get filled in!
            value_type_id=2, # This needs to get looked up too!
            index=pd.NA
        )
    )[VALUE_COLUMNS]

    database_table.to_sql(
        "value", get_engine("nvi_test"), if_exists="append", index=False
    )


if __name__ == "__main__":
    main()