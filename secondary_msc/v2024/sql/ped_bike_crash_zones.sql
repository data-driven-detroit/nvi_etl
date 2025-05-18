WITH
    neighborhood_zones AS (
        SELECT
            *
        FROM
            nvi.neighborhood_zones
        WHERE
            start_date = DATE '2026-01-01'
    ),
    crash AS (
        SELECT
            zones.zone_id,
            COUNT(*) AS crash_count
        FROM
            semcog_crash_20250317 AS cr
            INNER JOIN neighborhood_zones zones
            ON st_within(
                st_transform(st_setsrid (st_point (cr.xcord::NUMERIC, cr.ycord::NUMERIC), 4326), 2898),
                zones.geometry
            )
        WHERE
            RIGHT(cr.date_full, 4) = '2023'
            AND (
                cr.pedestrian = '1'
                OR cr.bicycle = '1'
            )
        GROUP BY
            zones.zone_id
    ),
    zone_crosswalk AS (
        SELECT DISTINCT
            tract_geoid,
            zone_name
        FROM
            nvi.tracts_to_nvi_crosswalk
        WHERE
            zone_start_date = DATE '2026-01-01'
    ),
    zone_population AS (
        SELECT
            zonec.zone_name,
            SUM(b01003001) AS total_pop
        FROM
            public.b01003_moe AS acs
            INNER JOIN zone_crosswalk zonec
                ON acs.geoid = zonec.tract_geoid
        GROUP BY
            zonec.zone_name
    )
SELECT
    'zone' AS geo_type,
    acs.zone_name AS geography,
    COALESCE(cr.crash_count, 0) AS count_ped_bike_crash,
    COALESCE(acs.total_pop, 1) AS universe_ped_bike_crash,
    (COALESCE(cr.crash_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS rate_ped_bike_crash,
    10000 AS per_ped_bike_crash
FROM
    zone_population AS acs
    LEFT JOIN crash AS cr ON acs.zone_name = cr.zone_id
ORDER BY
    acs.zone_name;
