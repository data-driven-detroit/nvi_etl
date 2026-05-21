"""Staging-table upsert for NVI value tables."""

import logging

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

logger = logging.getLogger("nvi_etl")


def _check_and_drop_duplicates(
    df: pd.DataFrame,
    table_name: str,
    key_cols: list[str],
) -> pd.DataFrame:
    """Detect duplicate key combinations, log diagnostics, drop them.

    Returns the deduplicated DataFrame (keeping first occurrence).
    If no duplicates, returns df unchanged.
    """
    duped_mask = df.duplicated(subset=key_cols, keep=False)
    if not duped_mask.any():
        return df

    duped_rows = df.loc[duped_mask, key_cols].copy()
    duped_rows["_row_index"] = duped_rows.index

    grouped = (
        duped_rows
        .groupby(key_cols, dropna=False)
        .agg(_row_index=("_row_index", list), _count=("_row_index", "size"))
        .reset_index()
        .sort_values("_count", ascending=False)
    )

    n_groups = len(grouped)
    n_dropped = int(duped_mask.sum()) - n_groups  # total dupes minus one kept per group

    logger.warning(
        f"\n{'='*60}\n"
        f"DUPLICATE KEYS in DataFrame for '{table_name}'\n"
        f"Key columns: {key_cols}\n"
        f"Duplicate groups: {n_groups} | Rows dropped: {n_dropped}\n"
        f"{'='*60}"
    )

    for _, row in grouped.head(20).iterrows():
        key_str = ", ".join(f"{col}={row[col]}" for col in key_cols)
        logger.warning(f"  [{row['_count']}x] {key_str}")

    if n_groups > 20:
        logger.warning(f"  ... and {n_groups - 20} more duplicate groups")

    return df.drop_duplicates(subset=key_cols, keep="first")


def _upsert(
    engine: Engine,
    df: pd.DataFrame,
    table_name: str,
    columns: list[str],
    key_cols: list[str],
    schema: str | None = None,
) -> int:
    """Generic staging-table upsert: delete matching rows then insert."""
    qualified_name = f"{schema}.{table_name}" if schema else table_name
    cols_csv = ", ".join(columns)

    where_clauses = " AND ".join(
        f"v.{col} IS NOT DISTINCT FROM s.{col}" for col in key_cols
    )

    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE TEMP TABLE _staging "
            f"ON COMMIT DROP "
            f"AS SELECT {cols_csv} FROM {qualified_name} WITH NO DATA"
        ))

        df[columns].to_sql("_staging", conn, if_exists="append", index=False)

        conn.execute(text(f"""
            WITH deleted AS (
                DELETE FROM {qualified_name} v
                USING _staging s
                WHERE {where_clauses}
                RETURNING 1
            )
            INSERT INTO {qualified_name} ({cols_csv})
            SELECT {cols_csv} FROM _staging
        """))

    return len(df)


def upsert_values(
    engine: Engine,
    df: pd.DataFrame,
    schema: str | None = None,
) -> int:
    """Upsert rows into the primary NVI value table."""
    key_cols = [
        "indicator_id",
        "location_id",
        "survey_id",
        "survey_question_id",
        "survey_question_option_id",
        "year",
    ]

    df = _check_and_drop_duplicates(df, SURVEY_VALUES_TABLE, key_cols)
    df = NVIValueTable.validate(df)

    return _upsert(engine, df, SURVEY_VALUES_TABLE, VALUE_COLUMNS, key_cols, schema=schema)


def upsert_context_values(
    engine: Engine,
    df: pd.DataFrame,
    schema: str | None = None,
) -> int:
    """Upsert rows into the context_values table."""
    key_cols = [
        "location_id",
        "indicator_id",
        "filter_type_id",
        "filter_option_id",
        "source_id",
        "start_date",
        "end_date",
    ]

    df = _check_and_drop_duplicates(df, CONTEXT_VALUES_TABLE, key_cols)
    df = NVIContextValueTable.validate(df)

    return _upsert(engine, df, CONTEXT_VALUES_TABLE, CONTEXT_VALUE_COLUMNS, key_cols, schema=schema)
