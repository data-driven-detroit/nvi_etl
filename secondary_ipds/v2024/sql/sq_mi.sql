WITH annotated AS (
    SELECT 
        cw.*,
        aland * 3.86102 * (10 ^ -7) AS sq_mi
    FROM nvi.tracts_to_nvi_crosswalk AS cw
    INNER JOIN shp.tiger_census_2020_tract_mi tracts
       ON cw.tract_geoid = '14000US' || tracts.geoid
)

SELECT
    'Detroit' AS geography,
    'citywide' AS geo_type,
    SUM(sq_mi) AS count_sq_mi
FROM annotated

UNION DISTINCT

SELECT
    zone_name AS geography,
    'zone' AS geo_type,
    SUM(sq_mi) AS count_sq_mi
FROM annotated
GROUP BY zone_name

UNION DISTINCT

SELECT
    district_number::text AS geography,
    'district' AS geo_type,
    SUM(sq_mi) AS count_sq_mi
FROM annotated
GROUP BY district_number;

