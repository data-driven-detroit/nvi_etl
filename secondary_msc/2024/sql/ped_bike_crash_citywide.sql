--- PED BIKE CRASH - CITYWIDE

WITH crash AS (
    SELECT count(*) AS ped_crash_count
    FROM semcog_crash_20250317 AS cr
    INNER JOIN shp.detroit_city_boundary_01182023 AS det
        ON
            st_intersects(
                det.geom,
                st_setsrid(st_point(cr.xcord::numeric, cr.ycord::numeric), 4326)
            )
    WHERE
        right(cr.date_full, 4) = '2023'
        AND (cr.pedestrian = '1' OR cr.bicycle = '1')
),

acs_population AS (
    SELECT b01003001 AS total_pop
    FROM public.b01003_moe
    WHERE geoid = '06000US2616322000'
)

SELECT
    'citywide' as geo_type,
    'citywide' as geography,
    cr.ped_crash_count AS total_ped_crash,
    acs.total_pop AS total_population,
    (cr.ped_crash_count * 10000.0 / nullif(acs.total_pop, 0)) AS crash_per_10000
FROM crash AS cr
CROSS JOIN acs_population AS acs;
