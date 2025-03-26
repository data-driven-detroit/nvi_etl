-- Auto Crashes
-- NVI Zones

WITH crash AS (
    SELECT nvi.district_n, nvi.zone_id, count(*) as crash_count
    FROM semcog_crash_20250317 as cr
    JOIN shp.nvi_neighborhood_zones_temp_2025 nvi
    ON ST_Intersects(st_transform(nvi.geom,4326), ST_SetSRID(ST_Point(cr.xcord::numeric, cr.ycord::numeric),4326))
    WHERE right(cr.date_full, 4) = '2023'
    GROUP BY nvi.district_n, nvi.zone_id
),
zone_population AS (
    SELECT nvi.district_n, nvi.zone_id, SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    JOIN shp.tiger_census_2020_tract_mi AS tract
    ON RIGHT(acs.geoid, 11) = tract.geoid
    JOIN shp.nvi_neighborhood_zones_temp_2025 nvi
    ON ST_Intersects(ST_Centroid(tract.geom), st_transform(nvi.geom,4326))
    GROUP BY nvi.district_n, nvi.zone_id
)
SELECT
    acs.district_n as council_districts,
    acs.zone_id as zone_id,
    COALESCE(cr.crash_count, 0) AS total_crash,
    COALESCE(acs.total_pop, 1) AS total_population,
    (COALESCE(cr.crash_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS crash_per_10000
FROM zone_population acs
LEFT JOIN crash cr
ON acs.district_n = cr.district_n and cr.zone_id = acs.zone_id
ORDER BY acs.district_n, acs.zone_id;

