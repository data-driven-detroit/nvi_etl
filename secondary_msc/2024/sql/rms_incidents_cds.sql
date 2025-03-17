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

