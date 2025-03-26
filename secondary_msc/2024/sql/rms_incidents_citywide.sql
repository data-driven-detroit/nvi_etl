--- Set to Data - Public
--- City of Detroit
WITH crime_incidents AS (
    SELECT *
    FROM rms_crime_incidents_20250311
    WHERE
        extract(YEAR FROM incident_occurred_at::date) = 2024
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
    SELECT count(*) AS rms_detroit
    FROM crime_incidents AS ci
    INNER JOIN shp.detroit_city_boundary_01182023 AS det
        ON
            st_intersects(
                det.geom, st_setsrid(st_point(ci.longitude, ci.latitude), 4326)
            )
),

acs_population AS (
    SELECT b01003001 AS total_pop
    FROM public.b01003_moe
    WHERE geoid = '06000US2616322000'
)

SELECT
    'citywide' as geo_type,
    'citywide' as geography,
    rms.rms_detroit AS total_violent_crimes,
    acs.total_pop AS total_population,
    (
        rms.rms_detroit * 10000.0 / nullif(acs.total_pop, 0)
    ) AS crime_rate_per_10000
FROM rms_crime_count AS rms
CROSS JOIN acs_population AS acs;
