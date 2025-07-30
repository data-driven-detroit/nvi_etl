import datetime
import asyncio
import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy.schema import DropTable

from nvi_etl import setup_logging, make_engine_for
from nvi_etl.destinations import CONTEXT_VALUES_TABLE, SURVEY_VALUES_TABLE


logger = setup_logging()
today = datetime.date.today().strftime("%Y%m%d")


async def run(command):
    proc = await asyncio.create_subprocess_exec(*command)
    await proc.wait()


async def run_all_pipelines():

    python = "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\env\\Scripts\\python"
    files = [
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\primary_survey\\v2024\\nvi_2024_processor.py",
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_acs\\v2024\\process_acs_2024.py",
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_ipds\\v2024\\process_ipds_2024.py",
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_mischooldata\\v2024\\process_mischooldata_2024.py",
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_msc\\v2024\\process_msc_2024.py",
    ]

    await asyncio.gather(*[run([python, file]) for file in files])


def run_everything():
    # Clear data from the destination databases
    logger.info("Dropping the context and value tables before beginning.")
    data_engine = make_engine_for("data")

    metadata = MetaData()

    survey_values = Table(
        SURVEY_VALUES_TABLE,
        metadata,
        schema="nvi"
    )

    context_values = Table(
        CONTEXT_VALUES_TABLE,
        metadata,
        schema="nvi"
    )

    with data_engine.begin() as db:
        db.execute(DropTable(survey_values, if_exists=True))
        db.execute(DropTable(context_values, if_exists=True))

    # Run each process to reload the tables

    asyncio.run(run_all_pipelines())


def collect_primary_output():
    output_files = [
       "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\primary_survey\\v2024\\output\\primary_survey_tall_2024.csv",
       "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_acs\\v2024\\output\\acs_primary_indicators_tall.csv",
       "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_ipds\\v2024\\output\\foreclosures_tall.csv",
       "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_ipds\\v2024\\output\\ipds_primary_tall_from_queries.csv",
       "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_mischooldata\\v2024\\output\\g3_ela_2023_tall.csv",
       "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_msc\\v2024\\output\\births_output_tall.csv",
       "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_msc\\v2024\\output\\msc_output_tall.csv",
    ]

    collected = (
        pd.concat([
            pd.read_csv(file).astype({
                "location_id": pd.Int64Dtype(),
                "indicator_id": pd.Int64Dtype(),
                "year": pd.Int64Dtype(),
                "dollars": pd.Int64Dtype(),
                "rate_per": pd.Int64Dtype(),
            }) for file in output_files
        ])
        .rename(columns={
            "percentage": "__percentage"
        })
        .dropna(subset="indicator_id")
        .fillna({"value_type_id": 1})
        .assign(
            percentage = lambda df: df["__percentage"] * 100,
            survey_id = 1, # THIS IS IMPORTANT THAT IT IS SET HERE
        )
        .drop("__percentage", axis=1).round({
            "count": 0,
            "percentage": 0,
            "universe": 0,
            "rate": 0,
            "index": 2,
        })
        .astype({
            "survey_question_id": pd.Int64Dtype(),
            "survey_question_option_id": pd.Int64Dtype(),
            "count": pd.Int64Dtype(),
            "universe": pd.Int64Dtype(),
            "percentage": pd.Int64Dtype(),
            "rate": pd.Int64Dtype(),
        })
        .reset_index(drop=True)
        .pipe(lambda df: df.rename_axis('id', axis=0))
    )[[
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
            "index"
        ]]

    (
        collected
        .sort_values("indicator_id")
        .to_csv(f"nvi_values_{today}.csv")
    )


def collect_context_output():
    output_files = [
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_acs\\v2024\\output\\acs_context_indicators_tall.csv",
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_acs_v2\\output\\acs_context_indicators_tall.csv",
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_ipds\\v2024\\output\\ipds_context_tall_from_queries.csv",
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_msc\\v2024\\output\\msc_context_tall_from_queries.csv",
        "C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\secondary_evictions\\output\\evictions.csv",
    ]

    db_column_order = [
        "count",
        "universe",
        "percentage",
        "filter_option_id",
        "indicator_id",
        "location_id",
        "source_id",
        "dollars",
        "index",
        "rate",
        "rate_per",
        "end_date",
        "start_date",
    ]

    collected = (
        pd.concat([pd.read_csv(file) for file in output_files])
        .reset_index()
        .rename(columns={
            "percentage": "__percentage"
        })
        .assign(percentage=lambda df: df["__percentage"] * 100)
        .round({
            "count": 0,
            "universe": 0,
            "percentage": 0,
            "rate": 0,
            "dollars": 0,
        })
        .astype({
            "location_id": pd.Int64Dtype(),
            "indicator_id": pd.Int64Dtype(),
            "year": pd.Int64Dtype(),
            "count": pd.Int64Dtype(), # This can't be coerced due to mischooldata suppression error
            "universe": pd.Int64Dtype(),
            "dollars": pd.Int64Dtype(),
            "rate": pd.Int64Dtype(),
            "rate_per": pd.Int64Dtype(),
            "filter_type_id": pd.Int64Dtype(),
            "filter_option_id": pd.Int64Dtype(),
        })[db_column_order]
    )

    collected.pipe(lambda df: df.rename_axis('id', axis=0)).to_csv(f"nvi_context_values_{today}.csv")


if __name__ == "__main__":
    # run_everything()
    # collect_primary_output()
    collect_context_output()
