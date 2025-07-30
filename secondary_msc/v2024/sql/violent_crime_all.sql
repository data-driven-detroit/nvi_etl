WITH violent_crime AS (
    SELECT DISTINCT ON (incident_entry_id) *
    FROM rms_crime_incidents_20250311 crimes
    JOIN nvi.neighborhood_zones zones
        ON ST_WITHIN(
            ST_TRANSFORM(
                ST_SETSRID(
                    ST_POINT(crimes.longitude, crimes.latitude), 
                    4326
                ), 
                2898
            ),
            zones.geometry
        )
    WHERE EXTRACT(YEAR from incident_occurred_at::date) = 2024
    AND zones.start_date = DATE '2026-01-01'
    AND TRIM(case_status) != 'UNFOUNDED'
    -- The offense_description matches are pulled from here
    AND TRIM(offense_description) IN (
        'AGGRAVATED / FELONIOUS ASSAULT',
        'AGGRAVATED / FELONIOUS ASSAULT (DOMESTIC)',
        'ASSAULT LESS THAN MURDER',
        'CSC 1ST DEGREE - OBJECT',
        'CSC 3RD DEGREE - OBJECT',
        'CSC 3RD DEGREE - ORAL / ANAL',
        'CSC 3RD DEGREE - PENIS / VAGINA',
        'CSC 1ST DEGREE - ORAL / ANAL',
        'CSC 1ST DEGREE - PENIS / VAGINA',
        'MURDER / NON-NEGLIGENT MANSLAUGHTER (VOLUNTARY)',
        'ROBBERY',
        'ROBBERY - ARMED',
        'ROBBERY - MOTOR VEHICLE (CARJACKING)',
        'ROBBERY - STRONG ARM',
        'ROBBERY - UNARMED'
    )
),
annotated_tracts AS (
    SELECT
        cw.zone_name,
        cw.district_number,
        b01003001 AS pop
    FROM
        b01003_moe AS acs
        INNER JOIN nvi.tracts_to_nvi_crosswalk cw
            ON acs.geoid = cw.tract_geoid
    WHERE
        cw.zone_start_date = DATE '2026-01-01'
),
district_population AS (
    SELECT
        district_number,
        SUM(pop) AS total_pop
    FROM annotated_tracts
    GROUP BY
        district_number
),
zone_population AS (
    SELECT
        zone_name,
        SUM(pop) AS total_pop
    FROM annotated_tracts
    GROUP BY
        zone_name
)
SELECT
    'citywide' AS geo_type,
    'Detroit' AS geography,
    COUNT(*) AS count_violent_crime,
    MAX(b01003001) AS universe_violent_crime,
    (COUNT(*) * 10000) / MAX(b01003001)  AS rate_violent_crime,
    10000 AS per_violent_crime
FROM violent_crime
CROSS JOIN b01003_moe
WHERE geoid = '06000US2616322000'

UNION DISTINCT

SELECT
    'district' AS geo_type,
    c.district_number::text AS geography,
    COUNT(*) AS count_violent_crime,
    MAX(dp.total_pop) AS universe_violent_crime,
    (COUNT(*) * 10000) / MAX(dp.total_pop)  AS rate_violent_crime,
    10000 AS per_violent_crime
FROM violent_crime c
JOIN district_population dp
    ON c.district_number = dp.district_number
GROUP BY c.district_number

UNION DISTINCT

SELECT
    'zone' AS geo_type,
    c.zone_id AS geography,
    COUNT(*) AS count_violent_crime,
    MAX(zp.total_pop) AS universe_violent_crime,
    (COUNT(*) * 10000) / MAX(zp.total_pop)  AS rate_violent_crime,
    10000 AS per_violent_crime
FROM violent_crime c
JOIN zone_population zp
    ON c.zone_id = zp.zone_name
GROUP BY c.zone_id;
