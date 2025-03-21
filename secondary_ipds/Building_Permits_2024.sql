--- Set to Data - Public
--- City of Detroit
WITH building_permits AS (
    SELECT COUNT(*) AS dp_detroit
    FROM detodp_building_permits as dp
    JOIN shp.detroit_city_boundary_01182023 det
    ON ST_Intersects(det.geom, ST_SetSRID(ST_Point(dp.lon, dp.lat), 4326))
),
acs_population AS (
    SELECT b01003001 AS total_pop
    FROM public.b01003_moe
    WHERE geoid = '06000US2616322000'
)
SELECT
    dp.dp_detroit AS total_building_permits,
    acs.total_pop AS total_population,
    (dp.dp_detroit * 10000.0 / NULLIF(acs.total_pop, 0)) AS building_permits_per_10000
FROM building_permits dp
CROSS JOIN acs_population acs;

--- Council District 2026
WITH building_permits AS (
    SELECT nvi.council_di, COUNT(*) AS building_permit_count
    FROM detodp_building_permits as dp
    INNER JOIN shp."Detroit_City_Council_Districts_2026" nvi
    ON st_intersects(st_transform(nvi.geom,4326), ST_SetSRID(ST_Point(dp.lon, dp.lat), 4326))
    GROUP BY nvi.council_di
),
zone_population AS (
    SELECT nvi.council_di, SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    JOIN shp.tiger_census_2020_tract_mi AS tract
    ON RIGHT(acs.geoid, 11) = tract.geoid
    INNER JOIN shp."Detroit_City_Council_Districts_2026" nvi
    ON ST_Intersects(ST_Centroid(tract.geom), st_transform(nvi.geom,4326)) -- Census tract population assigned to council districts
    GROUP BY nvi.council_di
)
SELECT
    acs.council_di as council_districts,
    COALESCE(rms.building_permit_count, 0) AS total_building_permits,
    COALESCE(acs.total_pop, 1) AS total_population,
    (COALESCE(rms.building_permit_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS building_permits_per_10000
FROM zone_population acs
LEFT JOIN building_permits rms
ON acs.council_di = rms.council_di
ORDER BY acs.council_di;

 --- NVI Zone
WITH building_permits AS (
    SELECT nvi.district_n, nvi.zone_id, COUNT(*) AS building_permit_count
    FROM detodp_building_permits as dp
    INNER JOIN shp.nvi_neighborhood_zones_temp_2025 nvi
    ON st_intersects(st_transform(nvi.geom,4326), ST_SetSRID(ST_Point(dp.lon, dp.lat), 4326))
    GROUP BY nvi.district_n, nvi.zone_id
),
zone_population AS (
    SELECT nvi.district_n, nvi.zone_id, SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    JOIN shp.tiger_census_2020_tract_mi AS tract
    ON RIGHT(acs.geoid, 11) = tract.geoid
    INNER JOIN shp.nvi_neighborhood_zones_temp_2025 nvi
    ON ST_Intersects(ST_Centroid(tract.geom), st_transform(nvi.geom,4326)) -- Census tract population assigned to council districts
    GROUP BY nvi.district_n, nvi.zone_id
)
SELECT
    acs.district_n, acs.zone_id as council_districts,
    COALESCE(rms.building_permit_count, 0) AS total_building_permits,
    COALESCE(acs.total_pop, 1) AS total_population,
    (COALESCE(rms.building_permit_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS building_permits_per_10000
FROM zone_population acs
LEFT JOIN building_permits rms
ON acs.district_n = rms.district_n and rms.zone_id = acs.zone_id
ORDER BY acs.district_n, acs.zone_id;