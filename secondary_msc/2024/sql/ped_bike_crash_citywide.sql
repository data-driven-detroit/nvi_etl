--- PED BIKE CRASH - CITYWIDE

WITH crash AS (
    SELECT count(*) as ped_crash_count
    FROM semcog_crash_20250317 as cr
    JOIN shp.detroit_city_boundary_01182023 det
    ON ST_Intersects(det.geom, ST_SetSRID(ST_Point(cr.xcord::numeric, cr.ycord::numeric),4326))
    WHERE right(cr.date_full, 4) = '2023' and (cr.pedestrian = '1' or cr.bicycle = '1')
),
acs_population AS (
    SELECT b01003001 AS total_pop
    FROM public.b01003_moe
    WHERE geoid = '06000US2616322000'
)
SELECT
    cr.ped_crash_count AS total_ped_crash,
    acs.total_pop AS total_population,
    (cr.ped_crash_count * 10000.0 / NULLIF(acs.total_pop, 0)) AS crash_per_10000
FROM crash cr
CROSS JOIN acs_population acs;

