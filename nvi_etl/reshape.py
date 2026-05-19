"""Data reshaping utilities for wide-to-tall transformations."""

import json
import pandas as pd
from sqlalchemy.types import Integer, Float, Numeric

from nvi_etl.config import CONF_DIR


def pull_instructions(name="liquefy_instructions.json"):
    """Load liquefy instructions from the conf directory."""
    return json.loads((CONF_DIR / name).read_text())


def nvi_table_dtypes():
    """Provide a default set of sqlalchemy datatypes based on the instructions file."""
    instructions = pull_instructions()

    type_dict = {
        "sqla_int": Integer(),
        "sqla_float": Float(),
        "sqla_dollar": Numeric(precision=10, scale=2),
    }

    return {key: type_dict[val] for key, val in instructions["dtypes"]}


def liquefy(df, instructions=None, defaults=None):
    """
    De-pivot a wide DataFrame to tall format using instruction mappings.

    Similar to pandas 'melt' but uses a JSON instruction file to map
    column names to indicator IDs and place values in the correct
    typed columns.
    """
    if instructions is None:
        instructions = pull_instructions()
    if defaults is None:
        defaults = {}

    type_map = {
        "percentage": pd.Float64Dtype(),
        "index": pd.Float64Dtype(),
        "count": pd.Int64Dtype(),
        "survey_id": pd.Int64Dtype(),
        "survey_question_id": pd.Int64Dtype(),
        "survey_question_option_id": pd.Int64Dtype(),
    }

    collector = {
        col: []
        for col in ["indicator_id"]
        + instructions["index_cols"]
        + list(instructions["dtypes"].keys())
    }

    for _, row in df.iterrows():
        for col, instruction in instructions["id_map"].items():
            if col not in row:
                continue

            collector["indicator_id"].append(instruction["id"])

            for index_col in instructions["index_cols"]:
                index_value = row.get(index_col, defaults.get(index_col))
                collector[index_col].append(index_value)

            for dtype in instructions["dtypes"]:
                if dtype == instruction["dtype"]:
                    collector[dtype].append(row[col])
                else:
                    collector[dtype].append(None)

    result = pd.DataFrame(collector)

    return result.astype(type_map)


def elongate(wide):
    """
    Reshape wide-format data to tall using pandas wide_to_long.

    Expects columns named like count_X, universe_X, percentage_X, etc.
    where X is the indicator name.
    """
    return (
        pd.wide_to_long(
            wide,
            stubnames=[
                "count",
                "universe",
                "percentage",
                "rate",
                "per",
                "dollars",
                "index",
            ],
            i=["location_id", "year"],
            j="indicator",
            sep="_",
            suffix=".*",
        )
        .reset_index()
        .rename(columns={"per": "rate_per"})
    )
