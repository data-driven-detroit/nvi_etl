# THE NVI ETL

The home for NVI ETL scripts.

TODO: Capture more notes from meeting with Johnson center about how this ETL package works. (I'm trying to capture as much as possible from mine - MV).

## Overview

Each year (or quarter, month, etc if ETL is run more than yearly) gets its own folder to deal with any changes in the source dataset. Each year folder has a file that runs the entire pipeline. For the NVI ETL for instance, that file is called nvi_2023_processor.py.

This 'processor' file basically calls a few functions, though Brian suggested that this is where logging, and notification via tools like email or Asana can also be built in. It looks like this:


```python
from nvi_etl import setup_logging

from extract_data import extract_data
from transform_data import transform_data
from load_data import load_data

# This is configured in ./logging_config.json
logger = setup_logging()

extract_data(logger)
transform_data(logger)
load_data(logger)

# TODO: Add back validate / archive steps
```

A short description of each of these steps follows.


### Extract

This function opens the data from a source file -- in the example it reads a config file to find the source directory, it then copies that file into the 'input' folder (we should discuss if this copy step is required or if we should just pull directly from the vault).


### Validate

This function checks the incoming data for potential issues before any transformation begins.


### Transformation

This is where most of the clean-up and transformation work happens.


**(We should talk about if we need output validation - MV)**


### Load Data

This loads the dataset into the destination database, or saves it to the destination file to be consumed by the next step in analysis.


### Archive Data

This step saves the output in cold storage or somewhere on the file system as a backup.


## File Structure

Notes on this: 

1. We may not need the 'input' dir since most of our raw files are either in the Vault or saved in DUA.
    a. Same with output? Let's discuss at the data meeting.

```
.
├── <year>
│   ├── conf
│   │   └── variable_indicators.csv
│   ├── input
│   ├── output
│   ├── extract_data.py
│   ├── load_data.py
│   ├── archive_data.py
│   ├── nvi_<year>_processor.py
│   ├── transform_data.py
│   └── validate_data.py
├── ... other years
├── metadata.toml <- (see below)
└── README.md
```


## Notes on data flow

For each topic area, the ETL scripts follow the pattern: 

1. *Pull* the data in a 'wide' format, where each row represents a single geography for a single year. The geographies pulled in the 'wide' format include Detroit Overall, Council Districts, and NVI Neighborhood zones. The 'key' for each row in this format are the fields 'geo_type' (either `citywide`, `district`, or `zone`), 'geography, the identifier for that geography type, and 'year' the year the data pertains to.

2. *Transform* the 'wide' file to the 'tall' format where each row is an indicator for a particular geography for a particular year. In the case of the primary_survey data, the 'key' of each row of the tall format also includes the `survey_id`, `survey_question_id`, and `survey_question_option_id`.


### The _tall_ output table

The final values table for the NVI website has the following fields:

```
- id                          ⎤ 
- indicator_id                ⎥
- location_id                 ⎥ -- key columns
- survey_id                   ⎥
- survey_question_id          ⎥
- survey_question_option_id   ⎦

- value_type_id               ] -- helper column*

- year                        ⎤
- count                       ⎥
- universe                    ⎥
- percentage                  ⎥ -- value columns
- rate                        ⎥ 
- rate_per                    ⎥ 
- dollars                     ⎥
- index                       ⎦
```

*This might be removed if we move all secondary data to the 'context_value' table.

For any value 'type' a combination of value columns must have values, and the rest will be `null`. For instance, the 'percentage' type must have values in the `percentage` column, but values should also be present in the `count` and `universe` columns. The data field naming scheme described below describes how to correctly assign the 


### Part 1. Building the _wide_ table

The wide table can be created in whatever way best suits the analysis. The only requirements are on the fields describing the geography, as well as the naming convention of the data fields. Both are described below.

#### The geography key fields

The wide format for the data should include as a key 'geo_type' and 'geography', where 'geo_type' is one of `citywide`, `district`, or `zone`, and a 'geography' column that will have the label of the given geography. Take a look at `nvi_etl/conf/location_mapping.json` to view the mapping between the geography labels and their database ids on the NVI site.

#### Labeling the data fields

Any data field created *must* adhere to the following standard to be easily transformed into the 'tall' format.

`<indicator_part>_<indicator_name>`

The first part of the data field name is the `indicator_part` and must be one of the following.

- count_*
- universe_*
- percentage_*
- rate_*
- per_*
- dollars_*
- index_*

These stems will be used to place the value in the corresponding column on the output table. 


For example, starting with the table below:

  geo_type           | geography  |  count_ | universe_ | percentage_ | ...
---------------------|------------|------------------|--------------------|----------------------|-----------
 'citywide'          | 'citywide' |  ...             | ...                | ...                  | ...
 'council_districts' | '2'        |  ...             | ...                | ...                  | ...
 'neighborhood_zones'| '5a'       |  ...             | ...                | ...                  | ...

Each data fieldname will be split and the transformation to the 'tall version will be 



The output table also has the `indicator_id` column. This is an integer on the NVI database, but in the wide file this should be a readable name to identify which groups of columns refer to the same indicator row.

`indicators.json`

```json
{
  "below_fpl": 3,
}
```



Once the location is 'pinned,' you can use the `liquefy` function (importable directly from nvi_etl) to turn this wide format table into the long format table used by the database. Make sure that the `nvi_etl/conf/liquefy_instructions.json` has the appropriate data needed to make the transformation.




## Data dependencies

- NVI
  - Zones were created by hand by Laura, along with the crosswalk to tracts
  - City Boundary from Detroit's Open Data Portal (geojson In the Vault)
  - 2026 City Council Districts from Detroit's Open Data Portal (shapefile In the Vault)
- ACS Data
    - [d3census](https://github.com/mikevatd3/d3census)
- IPDS
    - census table b01003 (total population) in public schema
    - building permits -- which file / table?
    - blight violations -- which file / table?
    - raw.valassis_vnefplus_mi_20241017_det
- Michigan School Data mostly managed by [mischooldata_etl](https://github.com/data-driven-detroit/mischooldata_etl.git)
    - `education.eem_schools` 
    - `education.g3_ela_school` (old schema)
