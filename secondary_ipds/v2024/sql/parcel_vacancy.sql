WITH annotated AS (
    SELECT
        zones.district_number,
        zones.zone_id,
        CASE WHEN COALESCE(num_bldgs = 0, FALSE) THEN 1 ELSE 0 END AS is_vacant,
        CASE WHEN COALESCE(LEFT(zoning, 1) = 'R', FALSE) THEN 1 ELSE 0 END AS is_residential,
        CASE WHEN COALESCE(taxpayer_1 = 'DETROIT LAND BANK AUTHORITY', TRUE) THEN 1 ELSE 0 END AS lb_owned
    FROM raw.detodp_assessor_20240205 AS parcels
    INNER JOIN nvi.neighborhood_zones AS zones
        ON
            ST_WITHIN(
                ST_CENTROID(ST_TRANSFORM(parcels.geom, 2898)), zones.geometry
            )
)

SELECT
    'Detroit' AS geography,
    'citywide' AS geo_type,
    SUM(is_vacant) AS count_vacant_parcels,
    COUNT(*) AS universe_vacant_parcels,
    SUM(lb_owned) AS count_lb_owned,
    COUNT(*) AS universe_lb_owned
FROM annotated

UNION DISTINCT

SELECT
    district_number::text AS geography,
    'district' AS geo_type,
    SUM(is_vacant) AS count_vacant_parcels,
    COUNT(*) AS universe_vacant_parcels,
    SUM(lb_owned) AS count_lb_owned,
    COUNT(*) AS universe_lb_owned
FROM annotated
GROUP BY district_number

UNION DISTINCT

SELECT
    zone_id AS geography,
    'zone' AS geo_type,
    SUM(is_vacant) AS count_vacant_parcels,
    COUNT(*) AS universe_vacant_parcels,
    SUM(lb_owned) AS count_lb_owned,
    COUNT(*) AS universe_lb_owned
FROM annotated
GROUP BY zone_id;
