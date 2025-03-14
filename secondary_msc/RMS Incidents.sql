--- Set to Data - Public
--- City of Detroit
WITH crime_incidents AS (
    SELECT *
    FROM rms_crime_incidents_20250311
    WHERE extract(year from incident_occurred_at::date) = 2024
    AND charge_description IN (
        'ROBBERY', 'AGGRAVATED / FELONIOUS ASSAULT', 'AGGRAVATED / FELONIOUS ASSAULT (DOMESTIC)',
        'CSC 1ST DEGREE - PENIS / VAGINA', 'CSC 3RD DEGREE - PENIS / VAGINA',
        'HOMICIDE - JUSTIFIABLE', 'MURDER - ATTEMPT', 'MURDER / NON-NEGLIGENT MANSLAUGHTER (VOLUNTARY)',
        'ROBBERY - ATTEMPT - ARMED', 'ROBBERY - STRONG ARM', 'ROBBERY - UNARMED'
    )
),
rms_crime_count AS (
    SELECT COUNT(*) AS rms_detroit
    FROM crime_incidents ci
    JOIN shp.detroit_city_boundary_01182023 det
    ON ST_Intersects(det.geom, ST_SetSRID(ST_Point(ci.longitude, ci.latitude), 4326))
),
acs_population AS (
    SELECT b01003001 AS total_pop
    FROM public.b01003_moe
    WHERE geoid = '06000US2616322000'
)
SELECT
    rms.rms_detroit AS total_violent_crimes,
    acs.total_pop AS total_population,
    (rms.rms_detroit * 10000.0 / NULLIF(acs.total_pop, 0)) AS crime_rate_per_10000
FROM rms_crime_count rms
CROSS JOIN acs_population acs;

--- Council District 2026
WITH crime_incidents AS (
    SELECT *
    FROM rms_crime_incidents_20250311
    WHERE EXTRACT(YEAR FROM incident_occurred_at::date) = 2024
    AND charge_description IN (
        'ROBBERY', 'AGGRAVATED / FELONIOUS ASSAULT', 'AGGRAVATED / FELONIOUS ASSAULT (DOMESTIC)',
        'CSC 1ST DEGREE - PENIS / VAGINA', 'CSC 3RD DEGREE - PENIS / VAGINA',
        'HOMICIDE - JUSTIFIABLE', 'MURDER - ATTEMPT', 'MURDER / NON-NEGLIGENT MANSLAUGHTER (VOLUNTARY)',
        'ROBBERY - ATTEMPT - ARMED', 'ROBBERY - STRONG ARM', 'ROBBERY - UNARMED'
    )
),
rms_crime_count AS (
    SELECT nvi.council_di, COUNT(*) AS rms_count
    FROM crime_incidents ci
    INNER JOIN shp."Detroit_City_Council_Districts_2026" nvi
    ON st_intersects(st_transform(nvi.geom,4326), ST_SetSRID(ST_Point(ci.longitude, ci.latitude), 4326))
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
    COALESCE(rms.rms_count, 0) AS total_violent_crimes,
    COALESCE(acs.total_pop, 1) AS total_population,
    (COALESCE(rms.rms_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS crime_rate_per_10000
FROM zone_population acs
LEFT JOIN rms_crime_count rms
ON acs.council_di = rms.council_di
ORDER BY acs.council_di;

 --- NVI Zone
 WITH crime_incidents AS (
    SELECT *
    FROM rms_crime_incidents_20250311
    WHERE EXTRACT(YEAR FROM incident_occurred_at::date) = 2024
    AND charge_description IN (
        'ROBBERY', 'AGGRAVATED / FELONIOUS ASSAULT', 'AGGRAVATED / FELONIOUS ASSAULT (DOMESTIC)',
        'CSC 1ST DEGREE - PENIS / VAGINA', 'CSC 3RD DEGREE - PENIS / VAGINA',
        'HOMICIDE - JUSTIFIABLE', 'MURDER - ATTEMPT', 'MURDER / NON-NEGLIGENT MANSLAUGHTER (VOLUNTARY)',
        'ROBBERY - ATTEMPT - ARMED', 'ROBBERY - STRONG ARM', 'ROBBERY - UNARMED'
    )
),
rms_crime_count AS (
    SELECT nvi.district_n, nvi.zone_id, COUNT(*) AS rms_count
    FROM crime_incidents ci
    INNER JOIN shp.nvi_neighborhood_zones_temp_2025 nvi
    ON st_intersects(st_transform(nvi.geom,4326), ST_SetSRID(ST_Point(ci.longitude, ci.latitude), 4326))
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
    COALESCE(rms.rms_count, 0) AS total_violent_crimes,
    COALESCE(acs.total_pop, 1) AS total_population,
    (COALESCE(rms.rms_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS crime_rate_per_10000
FROM zone_population acs
LEFT JOIN rms_crime_count rms
ON acs.district_n = rms.district_n and rms.zone_id = acs.zone_id
ORDER BY acs.district_n, acs.zone_id;