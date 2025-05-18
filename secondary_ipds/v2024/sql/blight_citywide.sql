--- City if Detroit
-- It's okay to use the entire file because the city only tracks city parcels
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
    'citywide' AS geo_type,
    'Detroit' AS geography,
    SUM(non_blight) AS count_non_blighted,
    COUNT(non_blight) AS universe_non_blighted,
    (SUM(non_blight)::FLOAT / COUNT(non_blight)) AS percentage_non_blighted
FROM no_blight;