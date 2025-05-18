--- NVI Zone
--- Council District 2026
WITH
    building_permits AS (
        SELECT
            zones.zone_id,
            COUNT(*) AS building_permit_count
        FROM
            raw.detodp_building_permits_20240131 AS dp
            INNER JOIN nvi.neighborhood_zones AS zones ON ST_INTERSECTS (ST_TRANSFORM (zones.geometry, 4326), ST_SETSRID (ST_POINT (dp.lon, dp.lat), 4326))
        WHERE
            zones.start_date = DATE '2026-01-01'
        GROUP BY
            zones.zone_id
    ),
    zone_population AS (
        SELECT
            cw.zone_name AS zone_id,
            SUM(b01003001) AS total_pop
        FROM
            nvi.b01003_moe AS acs
            INNER JOIN nvi.tracts_to_nvi_crosswalk cw ON acs.geoid = cw.tract_geoid
        WHERE
            cw.tract_start_date = DATE '2020-01-01'
            AND cw.zone_start_date = DATE '2026-01-01'
        GROUP BY
            cw.zone_name
    )
SELECT
    'zone' AS geo_type,
    zone_population.zone_id AS geography,
    permits.building_permit_count AS count_bld_permits,
    zone_population.total_pop AS universe_bld_permits,
    10000 AS per_bld_permits,
    (
        COALESCE(permits.building_permit_count, 0) * 10000.0 / NULLIF(zone_population.total_pop, 0)
    ) AS rate_bld_permits
FROM
    zone_population
    LEFT JOIN building_permits permits ON zone_population.zone_id::TEXT = permits.zone_id;