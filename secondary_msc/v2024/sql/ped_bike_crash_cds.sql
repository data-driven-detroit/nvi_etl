-- PED BIKE CRASH - COUNCIL DISTRICTS
WITH
    detroit_council_districts AS (
        SELECT
            *
        FROM
            nvi.detroit_council_districts
        WHERE
            start_date = DATE '2026-01-01'
    ),
    crash AS (
        SELECT
            districts.district_number,
            COUNT(*) AS crash_count
        FROM
            semcog_crash_20250317 AS cr
            INNER JOIN detroit_council_districts AS districts ON st_intersects (
                districts.geometry,
                st_transform (st_setsrid (st_point (cr.xcord::NUMERIC, cr.ycord::NUMERIC), 4326), 2898)
            )
        WHERE
            RIGHT(cr.date_full, 4) = '2023'
            AND (
                cr.pedestrian = '1'
                OR cr.bicycle = '1'
            )
        GROUP BY
            districts.district_number
    ),
    district_crosswalk AS (
        SELECT DISTINCT
            tract_geoid,
            district_number::TEXT
        FROM
            nvi.tracts_to_nvi_crosswalk
        WHERE
            zone_start_date = DATE '2026-01-01'
    ),
    zone_population AS (
        SELECT
            dc.district_number,
            SUM(b01003001) AS total_pop
        FROM
            public.b01003_moe AS acs
            INNER JOIN district_crosswalk dc ON acs.geoid = dc.tract_geoid
        GROUP BY
            dc.district_number
    )
SELECT
    'district' AS geo_type,
    acs.district_number AS geography,
    COALESCE(cr.crash_count, 0) AS count_ped_bike_crash,
    COALESCE(acs.total_pop, 1) AS universe_ped_bike_crash,
    (COALESCE(cr.crash_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS rate_ped_bike_crash,
    10000 AS per_ped_bike_crash
FROM
    zone_population AS acs
    LEFT JOIN crash AS cr ON acs.district_number = cr.district_number
ORDER BY
    acs.district_number;