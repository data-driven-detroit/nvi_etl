WITH located_buildings AS (
    SELECT *
    FROM msc.nvi_prop_conditions_2025 cond
    JOIN nvi.neighborhood_zones zones
        ON st_within(st_transform(cond.geom, 2898), zones.geometry)
)

SELECT
    'citywide' AS geo_type,
    'Detroit' AS geography,
    COUNT(*) AS universe_vacant_res_blds,
    SUM(vacant) AS count_vacant_res_blds,
    SUM(vacant)::numeric / COUNT(*) AS percentage_vacant_res_blds
FROM located_buildings

UNION DISTINCT

SELECT
    'district' AS geo_type,
    district_number::text AS geography,
    COUNT(*) AS universe_vacant_res_blds,
    SUM(vacant) AS count_vacant_res_blds,
    SUM(vacant)::numeric / COUNT(*) AS percentage_vacant_res_blds
FROM located_buildings
GROUP BY district_number

UNION DISTINCT

SELECT
    'zone' AS geo_type,
    zone_id AS geography,
    COUNT(*) AS universe_vacant_res_blds,
    SUM(vacant) AS count_vacant_res_blds,
    SUM(vacant)::numeric / COUNT(*) AS percentage_vacant_res_blds
FROM located_buildings
GROUP BY zone_id

ORDER BY geo_type, geography; -- not required
