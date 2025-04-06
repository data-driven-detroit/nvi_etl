# THE NVI ETL

The home for NVI ETL scripts.

- Check out [ETL setup instructions](https://github.com/data-driven-detroit/nvi_etl/blob/main/SETUP.md) for help getting the project running locally
- Checkout [ETL basics](https://github.com/data-driven-detroit/nvi_etl/blob/main/ETL_BASICS.md) for high-level overview of project structure

## How the NVI ETL works in broad strokes

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

Any data field created *must* adhere to the following standard to be transformed into the 'tall' format.

`<indicator_part>_<indicator_name>`

The first part of the data field name is the `indicator_part` and must be one of the following.

- count_*
- universe_*
- percentage_*
- rate_*
- per_*
- dollars_*
- index_*

These stems will be used to place the value in the corresponding column on the output table. The `indicator_name` portion of the field name will be placed as a string into a new column `indicator`.

For example, starting with the table below:

  geo_type           | geography  |  count_emp | universe_emp | percentage_emp | count_kids | count_cats
---------------------|------------|------------|--------------|----------------|------------|------------
 'citywide'          | 'citywide' |  ...       | ...          | ...            | ...        | ...
 'council_districts' | '2'        |  ...       | ...          | ...            | ...        | ...
 'neighborhood_zones'| '5a'       |  ...       | ...          | ...            | ...        | ...


The data field names will be split and the transformation of the table table will look like this:

  geo_type           | geography  | indicator | count | universe | percentage
---------------------|------------|-----------|-------|----------|------------
 'citywide'          | 'citywide' | 'emp'     | ...   | ...      | ...
 'citywide'          | 'citywide' | 'kids'    | ...   | null     | null
 'citywide'          | 'citywide' | 'cats'    | ...   | null     | null
 'council_districts' | '2'        | 'emp'     | ...   | ...      | ...
 'council_districts' | '2'        | 'kids'    | ...   | null     | null
 'council_districts' | '2'        | 'cats'    | ...   | null     | null
 'neighborhood_zones'| '5a'       | 'emp'     | ...   | ...      | ...
 'neighborhood_zones'| '5a'       | 'kids'    | ...   | null     | null
 'neighborhood_zones'| '5a'       | 'cats'    | ...   | null     | null


Before the output table is complete, the `location_id` and `indicator_id` column must be mapped from the `geo_type`, `geography` and `indicator` columns.


```json
{
  "below_fpl": 3,
}
```





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
