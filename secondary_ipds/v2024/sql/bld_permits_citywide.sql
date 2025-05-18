-- City of Detroit
WITH building_permits AS (
    -- no need to filter because the detroit building department only reports on Detroit
    SELECT COUNT(*) AS dp_detroit
    FROM raw.detodp_building_permits_20240131 AS dp
),
acs_population AS (
    SELECT b01003001 AS total_pop
    FROM nvi.b01003_moe
    WHERE geoid = '06000US2616322000'
)
SELECT
    'citywide' AS geo_type,
    'Detroit' AS geography,
    -- Should this be people or parcels?
    dp.dp_detroit AS count_bld_permits,
    NULLIF(acs_population.total_pop, 0) AS universe_bld_permits,
    (dp.dp_detroit * 10000.0 / NULLIF(acs_population.total_pop, 0)) AS rate_bld_permits,
    10000 AS per_bld_permits
FROM building_permits AS dp
CROSS JOIN acs_population;

