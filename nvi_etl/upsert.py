"""Staging-table upsert for NVI value tables."""

from sqlalchemy import Engine, text
import pandas as pd

from nvi_etl.schema import (
    NVIValueTable,
    NVIContextValueTable,
    VALUE_COLUMNS,
    CONTEXT_VALUE_COLUMNS,
    SURVEY_VALUES_TABLE,
    CONTEXT_VALUES_TABLE,
)


def _upsert(
    engine: Engine,
    df: pd.DataFrame,
    table_name: str,
    columns: list[str],
    key_cols: list[str],
) -> int:
    """Generic staging-table upsert: delete matching rows then insert."""
    cols_csv = ", ".join(columns)

    where_clauses = " AND ".join(
        f"v.{col} IS NOT DISTINCT FROM s.{col}" for col in key_cols
    )

    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE TEMP TABLE _staging "
            f"ON COMMIT DROP "
            f"AS SELECT {cols_csv} FROM {table_name} WITH NO DATA"
        ))

        df[columns].to_sql("_staging", conn, if_exists="append", index=False)

        conn.execute(text(f"""
            WITH deleted AS (
                DELETE FROM {table_name} v
                USING _staging s
                WHERE {where_clauses}
                RETURNING 1
            )
            INSERT INTO {table_name} ({cols_csv})
            SELECT {cols_csv} FROM _staging
        """))

    return len(df)


def upsert_values(engine: Engine, df: pd.DataFrame) -> int:
    """Upsert rows into the primary NVI value table."""
    df = NVIValueTable.validate(df)

    key_cols = [
        "indicator_id",
        "location_id",
        "survey_id",
        "survey_question_id",
        "survey_question_option_id",
        "year",
    ]

    return _upsert(engine, df, SURVEY_VALUES_TABLE, VALUE_COLUMNS, key_cols)


def upsert_context_values(engine: Engine, df: pd.DataFrame) -> int:
    """Upsert rows into the context_values table."""
    df = NVIContextValueTable.validate(df)

    key_cols = [
        "location_id",
        "indicator_id",
        "filter_type_id",
        "filter_option_id",
        "source_id",
        "start_date",
        "end_date",
    ]

    return _upsert(engine, df, CONTEXT_VALUES_TABLE, CONTEXT_VALUE_COLUMNS, key_cols)
