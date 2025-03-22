--- City of Detroit
WITH building_permits AS (
    SELECT COUNT(*) AS dp_detroit
    FROM detodp_building_permits AS dp
    INNER JOIN shp.detroit_city_boundary_01182023 AS det
        ON ST_INTERSECTS(det.geom, ST_SETSRID(ST_POINT(dp.lon, dp.lat), 4326))
),

acs_population AS (
    SELECT b01003001 AS total_pop
    FROM public.b01003_moe
    WHERE geoid = '06000US2616322000'
)

SELECT
    dp.dp_detroit AS total_building_permits,
    acs.total_pop AS total_population,
    -- Should this be people or parcels
    (
        dp.dp_detroit * 10000.0 / NULLIF(acs.total_pop, 0)
    ) AS building_permits_per_10000
FROM building_permits AS dp
CROSS JOIN acs_population AS acs;

