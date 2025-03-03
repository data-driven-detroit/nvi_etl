from pathlib import Path
import json
import logging
import logging.config
import pandas as pd
from sqlalchemy import create_engine
import tomli


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



def liquefy(df, instructions, defaults=dict()):
    # TODO This should be set into a conf somewhere.
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

    

