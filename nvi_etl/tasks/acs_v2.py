"""ACS part 2 -- tablecensus-based pipeline using Excel definitions."""

import pandas as pd
from sqlalchemy import Engine

from nvi_etl.config import CONF_DIR
from nvi_etl.db import get_engine
from nvi_etl.registry import task, TaskResult
from nvi_etl.reshape import elongate
from nvi_etl.aggregations import compile_indicators
from nvi_etl.geo import pull_tracts_to_nvi_crosswalk, pin_location
from nvi_etl.upsert import upsert_context_values


ACS_CONF_DIR = CONF_DIR.parent / "acs" / "conf"


def _diminish_geoid(geoid: str):
    """Remove extra zeros from Census geoid format."""
    first, second = geoid.split("US")
    return first[:5] + "US" + second


def _build_geography_groups(wide_file, source):
    """Aggregate tract-level data to zones, districts, and citywide."""
    wide_file["geoid"] = wide_file["geoid"].apply(_diminish_geoid)

    districts = (
        pull_tracts_to_nvi_crosswalk(source, 2020, 2026)
        .rename(columns={"tract_geoid": "geoid"})
        .astype({"geoid": "str"})
    )

    sum_cols = {
        col: "sum" for col in wide_file.columns
        if col not in {"Geography Name", "Year", "Release", "geoid"}
    }

    geographies = []
    for geo_type, agg in [("zone", "zone_name"), ("district", "district_number")]:
        geographies.append(
            districts
            .merge(wide_file, on="geoid", how="left")
            .rename(columns={agg: "geography", "Year": "year"})
            .groupby(["geography", "year"])
            .agg(sum_cols)
            .assign(geo_type=geo_type)
            .reset_index()
        )

    geographies.append(
        wide_file.query("geoid == '06000US2616322000'")
        .rename(columns={"Year": "year"})
        .groupby("year")
        .agg(sum_cols)
        .assign(geography="Detroit", geo_type="citywide")
        .reset_index()
    )

    return pd.concat(geographies)


@task("acs_v2", phase=1, description="ACS part 2 via tablecensus Excel definitions")
def run(source: Engine, target: Engine) -> TaskResult:
    import logging
    logger = logging.getLogger("nvi_etl")

    # Extract via tablecensus
    from tablecensus.assemble import assemble_from

    logger.info("Assembling NVI ACS part 2")
    assembled = assemble_from(ACS_CONF_DIR / "definitions.xlsx")

    # Build geographies
    geography_counts = _build_geography_groups(assembled, source)

    context_indicators = pd.read_csv(ACS_CONF_DIR / "context_indicator_ids.csv", index_col=False)
    indicators = compile_indicators(context_indicators, logger)

    wide_table = (
        geography_counts
        .astype({"geography": "str"})
        .assign(**indicators, location_id=lambda df: df.apply(pin_location, axis=1))
    )

    stub_names = ["count", "universe", "percentage", "rate", "per", "dollars", "index"]
    necessary_columns = [
        col for col in wide_table
        if col.split("_")[0] in stub_names
        or col in ["location_id", "year", "indicator", "geo_type", "geography"]
    ]

    tall = elongate(wide_table[necessary_columns])

    context_tall = (
        tall
        .merge(context_indicators, on=["indicator", "year"], how="right")
        .drop(["indicator", "geo_type", "geography", "indicator_type"], axis=1)
        .sort_values(["indicator_id", "location_id"])
    )

    # Load
    rows = upsert_context_values(target, context_tall)

    return TaskResult(task_name="acs_v2", rows_inserted=rows, success=True)
