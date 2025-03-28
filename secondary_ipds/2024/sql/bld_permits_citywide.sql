--- City of Detroit
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
    'citywide' AS geography,
    dp.dp_detroit AS total_building_permits,
    acs.total_pop AS total_population,
    -- Should this be people or parcels?
    (
        dp.dp_detroit * 10000.0 / NULLIF(acs.total_pop, 0)
    ) AS building_permits_per_10000
FROM building_permits AS dp
CROSS JOIN acs_population AS acs;

