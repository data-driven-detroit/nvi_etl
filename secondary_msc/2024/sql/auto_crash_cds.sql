-- Auto Crashes
-- Council District 2026

WITH crash AS (
    SELECT nvi.council_di, count(*) as crash_count
    FROM semcog_crash_20250317 as cr
    JOIN shp."Detroit_City_Council_Districts_2026" nvi
    ON ST_Intersects(st_transform(nvi.geom,4326), ST_SetSRID(ST_Point(cr.xcord::numeric, cr.ycord::numeric),4326))
    WHERE right(cr.date_full, 4) = '2023'
    GROUP BY nvi.council_di
),
zone_population AS (
    SELECT nvi.council_di, SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    JOIN shp.tiger_census_2020_tract_mi AS tract
    ON RIGHT(acs.geoid, 11) = tract.geoid
    INNER JOIN shp."Detroit_City_Council_Districts_2026" nvi
    ON ST_Intersects(ST_Centroid(tract.geom), st_transform(nvi.geom,4326))
    GROUP BY nvi.council_di
)
SELECT
    acs.council_di as council_districts,
    COALESCE(cr.crash_count, 0) AS total_crash,
    COALESCE(acs.total_pop, 1) AS total_population,
    (COALESCE(cr.crash_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS crash_per_10000
FROM zone_population acs
LEFT JOIN crash cr
ON acs.council_di = cr.council_di
ORDER BY acs.council_di;

