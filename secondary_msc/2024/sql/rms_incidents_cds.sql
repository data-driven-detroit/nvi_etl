--- Council District 2026
WITH crime_incidents AS (
    SELECT *
    FROM rms_crime_incidents_20250311
    WHERE
        EXTRACT(YEAR FROM incident_occurred_at::date) = 2024
        AND charge_description IN (
            'ROBBERY',
            'AGGRAVATED / FELONIOUS ASSAULT',
            'AGGRAVATED / FELONIOUS ASSAULT (DOMESTIC)',
            'CSC 1ST DEGREE - PENIS / VAGINA',
            'CSC 3RD DEGREE - PENIS / VAGINA',
            'HOMICIDE - JUSTIFIABLE',
            'MURDER - ATTEMPT',
            'MURDER / NON-NEGLIGENT MANSLAUGHTER (VOLUNTARY)',
            'ROBBERY - ATTEMPT - ARMED',
            'ROBBERY - STRONG ARM',
            'ROBBERY - UNARMED'
        )
),

rms_crime_count AS (
    SELECT
        nvi.council_di,
        COUNT(*) AS rms_count
    FROM crime_incidents AS ci
    INNER JOIN shp."Detroit_City_Council_Districts_2026" AS nvi
        ON
            ST_INTERSECTS(
                ST_TRANSFORM(nvi.geom, 4326),
                ST_SETSRID(ST_POINT(ci.longitude, ci.latitude), 4326)
            )
    GROUP BY nvi.council_di
),

zone_population AS (
    SELECT
        nvi.council_di,
        SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    INNER JOIN shp.tiger_census_2020_tract_mi AS tract
        ON RIGHT(acs.geoid, 11) = tract.geoid
    INNER JOIN shp."Detroit_City_Council_Districts_2026" AS nvi
        -- Census tract population assigned to council districts
        ON ST_INTERSECTS(ST_CENTROID(tract.geom), ST_TRANSFORM(nvi.geom, 4326))
    GROUP BY nvi.council_di
)

SELECT
    'council_districts' as geo_type,
    acs.council_di AS geography,
    COALESCE(rms.rms_count, 0) AS total_violent_crimes,
    COALESCE(acs.total_pop, 1) AS total_population,
    (
        COALESCE(rms.rms_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)
    ) AS crime_rate_per_10000
FROM zone_population AS acs
LEFT JOIN rms_crime_count AS rms
    ON acs.council_di = rms.council_di
ORDER BY acs.council_di;
