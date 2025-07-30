WITH building_permits AS (
    SELECT
        "Record ID" AS permit_id,
        zones.zone_id,
        zones.district_number
    FROM raw.detodp_building_permits_20250522 permits
    INNER JOIN nvi.neighborhood_zones zones
        ON
            ST_WITHIN(
                ST_TRANSFORM(
                    ST_SETSRID(
                        ST_POINT(permits."Longitude", permits."Latitude"), 
                        4326
                    ), 
                    2898
                ),
                zones.geometry
            )
    WHERE zones.start_date = DATE '2026-01-01'
    -- Should this be 'Submitted Date' instead?
    AND EXTRACT(YEAR FROM permits."Issued Date"::date) = 2024
),
city_pop AS (
    SELECT 
        b01003001 AS total_pop
    FROM nvi.b01003_moe
    WHERE geoid = '06000US2616322000'
),
district_pop AS (
    SELECT
        cw.district_number,
        SUM(b01003001) AS total_pop
    FROM 
        nvi.b01003_moe AS acs
        INNER JOIN nvi.tracts_to_nvi_crosswalk cw
            ON acs.geoid = cw.tract_geoid
    WHERE 
        cw.tract_start_date = DATE '2020-01-01'
        AND cw.zone_start_date = DATE '2026-01-01'
    GROUP BY 
        cw.district_number
),
zone_pop AS (
    SELECT
        cw.zone_name AS zone_id,
        SUM(b01003001) AS total_pop
    FROM
        nvi.b01003_moe AS acs
        INNER JOIN nvi.tracts_to_nvi_crosswalk cw 
            ON acs.geoid = cw.tract_geoid
    WHERE
        cw.tract_start_date = DATE '2020-01-01'
        AND cw.zone_start_date = DATE '2026-01-01'
    GROUP BY
        cw.zone_name
)
SELECT
    -- MAX is okay because all the rows for population after the cross-join
    -- are the same.
    'citywide' AS geo_type,
    'Detroit' AS geography,
    COUNT(*) AS count_bld_permits,
    NULLIF(MAX(city_pop.total_pop), 0) AS universe_bld_permits,
    (COUNT(*) * 10000.0 / NULLIF(MAX(city_pop.total_pop), 0)) AS rate_bld_permits,
    10000 AS per_bld_permits
FROM building_permits AS dp
CROSS JOIN city_pop
GROUP BY () -- GRAB ALL ROWS

UNION

SELECT
    'district' AS geo_type,
    permits.district_number::text AS geography,
    COUNT(*) AS count_bld_permits,
    NULLIF(MAX(district_pop.total_pop), 0) AS universe_bld_permits,
    (
        COUNT(*) * 10000.0
        / NULLIF(MAX(district_pop.total_pop), 0)
    ) AS rate_bld_permits,
    10000 AS per_bld_permits
FROM district_pop
LEFT JOIN building_permits permits
    ON district_pop.district_number = permits.district_number
GROUP BY permits.district_number

UNION

SELECT
    'zone' AS geo_type,
    permits.zone_id AS geography,
    COUNT(*) AS count_bld_permits,
    NULLIF(MAX(zone_pop.total_pop), 0) AS universe_bld_permits,
    (
        COUNT(*) * 10000.0
        / NULLIF(MAX(zone_pop.total_pop), 0)
    ) AS rate_bld_permits,
    10000 AS per_bld_permits
FROM zone_pop
LEFT JOIN building_permits permits
    ON zone_pop.zone_id = permits.zone_id
GROUP BY permits.zone_id
ORDER BY geo_type, geography;