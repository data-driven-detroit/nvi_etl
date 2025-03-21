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

#TODO: Add logging
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


`IGNORE THIS SECTION FOR NOW`
## `metadata.toml`

This is something that I've (Mike) have been working on for the last few months
as a way to track our dataset metadata. It basically follows the structure that
we use dataset tracking in the vault.

Each table has its own `metadata.toml` file. This file has three sections.

- Table details
- Variable list
- Editions dictionary

Here is an example file:

```toml
# At the top of the file are table details

name=""
description = ""
unit_of_analysis = ""
universe = ""
owner = ""
collector = ""
# collection_method = ""
collection_reason = ""
source_url = ""
# notes = ""
# use_conditions = ""
# cadence = ""

# Next is the variable list using the toml [[]] list notation

[[variables]]
name = "the_first_variable"
description = ""
parent_variable = ""
# suppression_threshold = ""
# standard = "" # This is if there is some standard that might be used across, say, government datasets

[[variables]]
name = "another_variable"
description = "The name of the industry associated with the code"

# ... etc more variables here

# The Editions dictionary 
# Keeps track of which source file matches to which date

[tables.codes.editions.2024-10-01]
edition_date = "2024-10-01"

# Any notes that go with the edition specifically.
notes = "First upload of this dataset"

 # Path to the raw file REQUIRED
raw_path = "path/to/raw/file.csv"

start = "2024-01-01" 
end = "9999-12-31" # This is the 'forever' standard. This should be 

published = "2025-01-01"
acquired = "2025-01-01" 
```

This file is where to go in the 'load' file to find the raw path, it should also be referenced in the transform file
to append the appropriate start dates. TODO: This can be connected to a package that I've been working on 'metadata audit' which makes sure any file you're adding to the database has every field annotated that way we have that information for downstream analysis.


## Important data dependencies not managed by this repository

- NVI Zones
  - These were created by hand by Laura, along with the crosswalk
- ACS Data
    - [d3census](https://github.com/mikevatd3/d3census)
- IPDS
  - 
- Michigan School Data mostly managed by [mischooldata_etl](https://github.com/data-driven-detroit/mischooldata_etl.git)

