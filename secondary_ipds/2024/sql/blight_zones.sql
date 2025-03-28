--- NVI ZONES
WITH no_blight AS (
    SELECT CASE
              WHEN blight IS NULL
              AND CONDITION IS NULL
              AND vacant = 0 THEN 1
              ELSE 0
           END AS non_blight,
           geom
    FROM msc.nvi_prop_conditions_2025
)
SELECT
    'zone' AS geo_type,
    zones.zone_id AS geography,
    (SUM(non_blight) * 100 / COUNT(q1.geom)) AS pct_non_blighted
FROM no_blight
JOIN nvi.neighborhood_zones zones
    ON ST_WITHIN(ST_CENTROID(ST_TRANSFORM(no_blight.geom, 4326)), zones.geometry)
GROUP BY zones.zone_id;
