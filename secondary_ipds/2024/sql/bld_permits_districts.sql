--- Council District 2026
WITH building_permits AS (
    SELECT
        cds.district_number,
        COUNT(*) AS building_permit_count
    FROM raw.detodp_building_permits_20240131 AS dp
    INNER JOIN nvi.detroit_council_districts AS cds
        ON
            ST_INTERSECTS(
                ST_TRANSFORM(cds.geometry, 4326),
                ST_SETSRID(ST_POINT(dp.lon, dp.lat), 4326)
            )
    WHERE cds.start_date = DATE '2026-01-01'
    GROUP BY cds.district_number
),

district_pop AS (
    SELECT
        cw.district_number,
        SUM(b01003001) AS total_pop
    FROM nvi.b01003_moe AS acs
    INNER JOIN nvi.tracts_to_nvi_crosswalk cw
        ON acs.geoid = cw.tract_geoid
    WHERE cw.tract_start_date = DATE '2020-01-01'
    AND cw.zone_start_date = DATE '2026-01-01'
    GROUP BY cw.district_number
)

SELECT
    'district' AS geo_type,
    district_pop.district_number AS geography,
    (
        COALESCE(permits.building_permit_count, 0)
        * 10000.0
        / NULLIF(district_pop.total_pop, 0)
    ) AS building_permits_per_10000
FROM district_pop
LEFT JOIN building_permits permits
    ON district_pop.district_number::TEXT = permits.district_number;
