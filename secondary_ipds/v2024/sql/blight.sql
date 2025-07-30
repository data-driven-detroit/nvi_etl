WITH
    no_blight AS (
        SELECT
            CASE
                WHEN blight IS NULL
                AND CONDITION IS NULL
                AND vacant = 0 THEN 1
                ELSE 0
            END AS non_blight,
            geom
        FROM
            msc.nvi_prop_conditions_2025
    )
SELECT
    'citywide' AS geo_type,
    'Detroit' AS geography,
    SUM(non_blight) AS count_non_blighted,
    COUNT(non_blight) AS universe_non_blighted,
    (SUM(non_blight)::FLOAT / COUNT(non_blight)) AS percentage_non_blighted
FROM no_blight

UNION

SELECT
    'district' AS geo_type,
    cds.district_number AS geography,
    SUM(non_blight) AS count_non_blighted,
    COUNT(non_blight) AS universe_non_blighted,
    (SUM(non_blight)::FLOAT / COUNT(non_blight)) AS percentage_non_blighted
FROM
    no_blight
    JOIN nvi.detroit_council_districts cds ON ST_WITHIN (ST_CENTROID (ST_TRANSFORM (no_blight.geom, 2898)), cds.geometry)
WHERE
    cds.start_date = DATE '2026-01-01'
GROUP BY
    cds.district_number

UNION

SELECT
    'zone' AS geo_type,
    zones.zone_id AS geography,
    SUM(non_blight) AS count_non_blighted,
    COUNT(non_blight) AS universe_non_blighted,
    (SUM(non_blight)::FLOAT / COUNT(non_blight)) AS percentage_non_blighted
FROM no_blight
JOIN nvi.neighborhood_zones zones
    ON ST_WITHIN(ST_CENTROID(ST_TRANSFORM(no_blight.geom, 2898)), zones.geometry)
WHERE 
    zones.start_date = DATE '2026-01-01'
GROUP BY 
    zones.zone_id;
