# THE NVI ETL

The home for NVI ETL scripts.

TODO: Capture more notes from meeting with Johnson center about how this ETL package works. (I'm trying to capture as much as possible from mine - MV).

## Overview

Each year (or quarter, month, etc if ETL is run more than yearly) gets its own folder to deal with any changes in the source dataset. Each year folder has a file that runs the entire pipeline. For the NVI ETL for instance, that file is called nvi_2023_processor.py.

This 'processor' file basically calls a few functions, though Brian suggested that this is where logging, and notification via tools like email or Asana can also be built in. It looks like this:


```python
from extract_data import extract_data
from validate_data import validate_data
from transform_data import transform_data
from load_data import load_data
from archive_data import archive_data

#TODO: Add logging
extract_data()
validate_data()
transform_data()
load_data()
archive_data()
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
└── README.md
```

