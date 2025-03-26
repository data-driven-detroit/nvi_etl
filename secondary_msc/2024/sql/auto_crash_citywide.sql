-- Auto Crashes
-- Detroit

WITH crash AS (
    SELECT count(*) AS crash_count
    FROM semcog_crash_20250317 AS cr
    INNER JOIN shp.detroit_city_boundary_01182023 AS det
        ON
            st_intersects(
                det.geom,
                st_setsrid(st_point(cr.xcord::numeric, cr.ycord::numeric), 4326)
            )
    WHERE right(cr.date_full, 4) = '2023'
),

acs_population AS (
    SELECT b01003001 AS total_pop
    FROM public.b01003_moe
    WHERE geoid = '06000US2616322000'
)

SELECT
    'citywide' AS geo_type,
    'citywide' AS geography,
    cr.crash_count AS total_crash,
    acs.total_pop AS total_population,
    (cr.crash_count * 10000.0 / nullif(acs.total_pop, 0)) AS crash_per_10000
FROM crash AS cr
CROSS JOIN acs_population AS acs;
