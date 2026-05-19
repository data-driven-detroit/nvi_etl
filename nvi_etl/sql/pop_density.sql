WITH annotated AS (
    SELECT 
        cw.*,
        pop.b01003001,
        aland * 3.86102 * (10 ^ -7) AS sq_mi
    FROM nvi.b01003_moe AS pop
    INNER JOIN nvi.tracts_to_nvi_crosswalk AS cw
        ON pop.geoid = cw.tract_geoid
    INNER JOIN shp.tiger_census_2020_tract_mi tracts
       ON pop.geoid = '14000US' || tracts.geoid
)

SELECT
    'Detroit' AS geography,
    'citywide' AS geo_type,
    SUM(b01003001) AS count_pop_density,
    SUM(sq_mi) AS universe_pop_density,
    SUM(b01003001) / SUM(sq_mi) AS rate_pop_density,
    1 AS per_pop_density
FROM annotated

UNION DISTINCT

SELECT
    zone_name AS geography,
    'zone' AS geo_type,
    SUM(b01003001) AS count_pop_density,
    SUM(sq_mi) AS universe_pop_density,
    SUM(b01003001) / SUM(sq_mi) AS rate_pop_density,
    1 AS per_pop_density
FROM annotated
GROUP BY zone_name

UNION DISTINCT

SELECT
    district_number::text AS geography,
    'district' AS geo_type,
    SUM(b01003001) AS count_pop_density,
    SUM(sq_mi) AS universe_pop_density,
    SUM(b01003001) / SUM(sq_mi) AS rate_pop_density,
    1 AS per_pop_density
FROM annotated
GROUP BY district_number;

