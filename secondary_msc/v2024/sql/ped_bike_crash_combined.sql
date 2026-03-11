WITH 
city_crash AS (
    SELECT count(*) AS ped_crash_count
    FROM semcog_crash_20250317 AS cr
    INNER JOIN shp.detroit_city_boundary_01182023 AS det
        ON st_intersects(det.geom, st_setsrid(st_point(cr.xcord::numeric, cr.ycord::numeric), 4326))
    WHERE right(cr.date_full, 4)::int = :data_year
      AND (cr.pedestrian = '1' OR cr.bicycle = '1')
),
city_acs_population AS (
    SELECT b01003001 AS total_pop
    FROM public.b01003_moe
    WHERE geoid = '06000US2616322000'
),
detroit_council_districts AS (
    SELECT * FROM nvi.detroit_council_districts WHERE start_date = :geom_date
),
district_crash AS (
    SELECT districts.district_number, COUNT(*) AS crash_count
    FROM semcog_crash_20250317 AS cr
    INNER JOIN detroit_council_districts AS districts 
        ON st_intersects (districts.geometry, st_transform (st_setsrid (st_point (cr.xcord::NUMERIC, cr.ycord::NUMERIC), 4326), 2898))
    WHERE RIGHT(cr.date_full, 4) = '2023' AND (cr.pedestrian = '1' OR cr.bicycle = '1')
    GROUP BY districts.district_number
),
district_crosswalk AS (
    SELECT DISTINCT tract_geoid, district_number::TEXT
    FROM nvi.tracts_to_nvi_crosswalk WHERE zone_start_date = :geom_date
),
district_population AS (
    SELECT dc.district_number, SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    INNER JOIN district_crosswalk dc ON acs.geoid = dc.tract_geoid
    GROUP BY dc.district_number
),
neighborhood_zones AS (
    SELECT * FROM nvi.neighborhood_zones WHERE start_date = :geom_date
),
zone_crash AS (
    SELECT zones.zone_id, COUNT(*) AS crash_count
    FROM semcog_crash_20250317 AS cr
    INNER JOIN neighborhood_zones zones
        ON st_within(st_transform(st_setsrid (st_point (cr.xcord::NUMERIC, cr.ycord::NUMERIC), 4326), 2898), zones.geometry)
    WHERE RIGHT(cr.date_full, 4)::int = :data_year AND (cr.pedestrian = '1' OR cr.bicycle = '1')
    GROUP BY zones.zone_id
),
zone_crosswalk AS (
    SELECT DISTINCT tract_geoid, zone_name
    FROM nvi.tracts_to_nvi_crosswalk WHERE zone_start_date = :geom_date
),
zone_population AS (
    SELECT zonec.zone_name, SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    INNER JOIN zone_crosswalk zonec ON acs.geoid = zonec.tract_geoid
    GROUP BY zonec.zone_name
)
SELECT
    'citywide' AS geo_type,
    'Detroit' AS geography,
    cr.ped_crash_count AS count_ped_bike_crash,
    acs.total_pop AS universe_ped_bike_crash,
    (cr.ped_crash_count * 10000.0 / nullif(acs.total_pop, 0)) AS rate_ped_bike_crash,
    10000 AS per_ped_bike_crash
FROM city_crash AS cr CROSS JOIN city_acs_population AS acs
UNION ALL
SELECT
    'district' AS geo_type,
    acs.district_number AS geography,
    COALESCE(cr.crash_count, 0) AS count_ped_bike_crash,
    COALESCE(acs.total_pop, 1) AS universe_ped_bike_crash,
    (COALESCE(cr.crash_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS rate_ped_bike_crash,
    10000 AS per_ped_bike_crash
FROM district_population AS acs
LEFT JOIN district_crash AS cr ON acs.district_number = cr.district_number
UNION ALL
SELECT
    'zone' AS geo_type,
    acs.zone_name AS geography,
    COALESCE(cr.crash_count, 0) AS count_ped_bike_crash,
    COALESCE(acs.total_pop, 1) AS universe_ped_bike_crash,
    (COALESCE(cr.crash_count, 0) * 10000.0 / NULLIF(acs.total_pop, 0)) AS rate_ped_bike_crash,
    10000 AS per_ped_bike_crash
FROM zone_population AS acs
LEFT JOIN zone_crash AS cr ON acs.zone_name = cr.zone_id
ORDER BY geo_type, geography;
