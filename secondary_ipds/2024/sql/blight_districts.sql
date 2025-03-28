--- CD 2026
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
    'district' AS geo_type,
    cds.district_number AS geography,
    (SUM(non_blight) * 100 / COUNT(q1.geom)) AS pct_non_blighted
FROM no_blight
JOIN nvi.detroit_city_council_districts cds
    ON ST_WITHIN(ST_CENTROID(ST_TRANSFORM(no_blight.geom, 4326)), cds.geometry)
GROUP BY cds.district_number;
