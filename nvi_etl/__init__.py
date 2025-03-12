from pathlib import Path
import json
import logging
import logging.config
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, Numeric
import tomli


WORKING_DIR = Path(__file__).resolve().parent


with open(Path().cwd() / "config.toml", "rb") as f:
    config = tomli.load(f)


db_engine = create_engine(
    f"postgresql+psycopg://{config['db']['user']}:{config['db']['password']}"
    f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
    connect_args={'options': f'-csearch_path={config["app"]["name"]},public'},
)

metadata_engine = create_engine(
    f"postgresql+psycopg://{config['db']['user']}:{config['db']['password']}"
    f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
    connect_args={'options': f'-csearch_path={config["db"]["metadata_schema"]},public'},
)


def setup_logging():
    with open(Path.cwd() / "logging_config.json") as f:
        logging_config = json.load(f)

    logging.config.dictConfig(logging_config)

    return logging.getLogger(config["app"]["name"])


def pull_instructions():
    return json.loads(
        (WORKING_DIR / "conf" / "liquefy_instructions.json").read_text()
    )


def nvi_table_dtypes():
    """
    This provides a default set of sqlalchemy datatypes based on the 
    instructions file.
    """
    instructions = json.loads(
        (WORKING_DIR / "conf" / "liquefy_instructions.json").read_text()
    )

    type_dict = {
        "sqla_int": Integer(),
        "sqla_float": Float(),
        "sqla_dollar": Numeric(precision=10, scale=2), 
    }


    return {
        key: type_dict[val] for key, val in instructions["dtypes"]
    }


def liquefy(df, instructions=pull_instructions(), defaults=dict()):
    # TODO This should be set into a conf somewhere. Almost there!
    type_map = {
        "percentage": pd.Float64Dtype(),
        "index": pd.Float64Dtype(),
        "count": pd.Int64Dtype(),
        "survey_id": pd.Int64Dtype(),
        "survey_question_id": pd.Int64Dtype(),
        "survey_question_option_id": pd.Int64Dtype()
    }

    collector = {
        col: [] 
        for col in ["indicator_id"] + instructions["index_cols"] + instructions["dtypes"]
    }

    for _, row in df.iterrows():
        # Iterate over the columns following the 'instructions' 
        for col, instruction in instructions["id_map"].items():
            if col not in row:
                # Not all cols are available in every transformation
                continue

            collector["indicator_id"].append(instruction["id"])

            # For the 'index' columns, append the values directly, because these
            # will need to appear on every row.             
            for index_col in instructions["index_cols"]:
                # Fallback to the defaults dict if the column isn't in the dataframe 
                index_value = row.get(index_col, defaults.get(index_col))

                collector[index_col].append(index_value)

            for dtype in instructions["dtypes"]:
                # For the indicator value place it in the correct column based
                # on the instructions.
                if dtype == instruction["dtype"]:
                    collector[dtype].append(row[col])
                else:
                    collector[dtype].append(None)

    result = pd.DataFrame(collector)

    return result.astype(type_map)

    

