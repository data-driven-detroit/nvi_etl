--- NVI Zone
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
        nvi.district_n,
        nvi.zone_id,
        COUNT(*) AS rms_count
    FROM crime_incidents AS ci
    INNER JOIN shp.nvi_neighborhood_zones_temp_2025 AS nvi
        ON
            ST_INTERSECTS(
                ST_TRANSFORM(nvi.geom, 4326),
                ST_SETSRID(ST_POINT(ci.longitude, ci.latitude), 4326)
            )
    GROUP BY nvi.district_n, nvi.zone_id
),

zone_population AS (
    SELECT
        nvi.district_n,
        nvi.zone_id,
        SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    INNER JOIN shp.tiger_census_2020_tract_mi AS tract
        ON RIGHT(acs.geoid, 11) = tract.geoid
    INNER JOIN shp.nvi_neighborhood_zones_temp_2025 AS nvi
        -- Census tract population assigned to council districts
        ON ST_INTERSECTS(ST_CENTROID(tract.geom), ST_TRANSFORM(nvi.geom, 4326))
    GROUP BY nvi.district_n, nvi.zone_id
)

SELECT
<<<<<<< HEAD
    'neighborhood_zones' as geo_type,
=======
    'zone' as geo_type,
>>>>>>> main
    acs.zone_id AS geography,
    COALESCE(rms.rms_count, 0) AS total_violent_crimes,
    COALESCE(acs.total_pop, 1) AS total_population,
    (
        COALESCE(rms.rms_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)
    ) AS crime_rate_per_10000
FROM zone_population AS acs
LEFT JOIN rms_crime_count AS rms
    ON acs.district_n = rms.district_n AND acs.zone_id = rms.zone_id
ORDER BY acs.district_n, acs.zone_id;
