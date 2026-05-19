# NVI ETL

The Neighborhood Vitality Index data pipeline. Loads survey data and secondary
indicators into a PostgreSQL database for the NVI website.

## Quick start

```bash
# Install
uv sync

# Configure
cp .env.example .env
# Edit .env with your database credentials

# List available tasks
nvi-etl list

# Run a single task
nvi-etl run --task mischooldata

# Run all tasks in a phase
nvi-etl run --phase 1

# Run everything
nvi-etl run --all

# Override databases
nvi-etl run --task acs --source-db data --target-db nvi_test
```

## Project structure

```
nvi_etl/
  __init__.py
  cli.py                   # CLI entry point (nvi-etl)
  config.py                # .env config, path constants, logging setup
  db.py                    # SQLAlchemy engine factory
  registry.py              # @task decorator and task runner
  upsert.py                # Staging-table upsert for value tables
  schema.py                # Pandera validation schemas
  geo.py                   # Geographic data utilities
  reshape.py               # Wide-to-tall transformations (elongate, liquefy)
  utilities.py             # Shared helpers
  aggregations.py          # Indicator compilation functions
  conf/                    # Shared + per-task config files
  sql/                     # All SQL queries
  survey/                  # Primary survey support code
  acs/                     # ACS variable definitions and config
  tasks/                   # One module per ETL domain (@task decorated)
tests/                     # pytest test suite
archive/                   # Old versioned code for reference
```

## Tasks

Tasks are organized into phases:

**Phase 1** (database-to-database):
- `acs` -- ACS Census data via d3census
- `acs_v2` -- ACS part 2 via tablecensus Excel definitions
- `evictions` -- Eviction counts by geography and year
- `geography_*` -- Geographic boundary loading (zones, districts, boundary, crosswalk, CDO, HOLC)
- `ipds` -- Infrastructure, property, development & safety indicators
- `mischooldata` -- 3rd grade ELA proficiency from MI school data
- `msc` -- Births, crashes, crime, CDO coverage, redlining

**Phase 2** (file-based):
- `primary_survey` -- Primary NVI survey indicators and questions

## How the ETL works

For each topic area, the ETL follows the pattern:

1. **Extract**: Pull data in a 'wide' format where each row represents a single
   geography (citywide, district, or zone) for a single year.

2. **Transform**: Convert to 'tall' format where each row is an indicator for a
   particular geography and year.

3. **Load**: Validate with Pandera schemas and insert into the database.

### Data field naming convention

Data fields must follow `<value_type>_<indicator_name>`:
- `count_*`, `universe_*`, `percentage_*`, `rate_*`, `per_*`, `dollars_*`, `index_*`

These stems place values in the corresponding output columns. The indicator name
maps to an `indicator_id` via config files.

## Data dependencies

- **NVI**: Zones, crosswalks, city boundary, council districts
- **ACS**: via [d3census](https://github.com/mikevatd3/d3census)
- **IPDS**: Population (B01003), building permits, blight violations, Valassis
- **MI School Data**: via [mischooldata_etl](https://github.com/data-driven-detroit/mischooldata_etl.git)
