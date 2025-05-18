--- Neighborhood Zones 2026
WITH
    neighborhood_zones AS (
        SELECT
            *
        FROM
            nvi.neighborhood_zones
        WHERE
            start_date = DATE '2026-01-01'
    ),
    crime_incidents AS (
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
            zones.zone_id,
            COUNT(*) AS rms_count
        FROM crime_incidents AS ci
        INNER JOIN neighborhood_zones zones
            ON
                ST_INTERSECTS(
                    zones.geometry,
                    ST_TRANSFORM(ST_SETSRID(ST_POINT(ci.longitude, ci.latitude), 4326), 2898)
                )
        GROUP BY zones.zone_id
    ),
    zone_crosswalk AS (
        SELECT DISTINCT
            tract_geoid,
            zone_name
        FROM
            nvi.tracts_to_nvi_crosswalk
        WHERE
            zone_start_date = DATE '2026-01-01'
    ),
    zone_population AS (
        SELECT
            zones.zone_name,
            SUM(b01003001) AS total_pop
        FROM
            public.b01003_moe AS acs
            INNER JOIN zone_crosswalk zones ON acs.geoid = zones.tract_geoid
        GROUP BY
            zones.zone_name
    )
SELECT
    'zone' AS geo_type,
    acs.zone_name AS geography,
    COALESCE(rms.rms_count, 0) AS count_violent_crime,
    COALESCE(acs.total_pop, 1) AS universe_violent_crime,
    COALESCE(rms.rms_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0) AS rate_violent_crime,
    10000 AS per_violent_crime
FROM zone_population AS acs
LEFT JOIN rms_crime_count AS rms
    ON acs.zone_name = rms.zone_id
ORDER BY acs.zone_name;
