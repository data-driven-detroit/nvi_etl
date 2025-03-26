-- PED BIKE CRASH - NVI ZONES

WITH crash AS (
    SELECT
        nvi.district_n,
        nvi.zone_id,
        count(*) AS ped_crash_count
    FROM semcog_crash_20250317 AS cr
    INNER JOIN shp.nvi_neighborhood_zones_temp_2025 AS nvi
        ON
            st_intersects(
                st_transform(nvi.geom, 4326),
                st_setsrid(st_point(cr.xcord::numeric, cr.ycord::numeric), 4326)
            )
    WHERE
        right(cr.date_full, 4) = '2023'
        AND (cr.pedestrian = '1' OR cr.bicycle = '1')
    GROUP BY nvi.district_n, nvi.zone_id
),

zone_population AS (
    SELECT
        nvi.district_n,
        nvi.zone_id,
        sum(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    INNER JOIN shp.tiger_census_2020_tract_mi AS tract
        ON right(acs.geoid, 11) = tract.geoid
    INNER JOIN shp.nvi_neighborhood_zones_temp_2025 AS nvi
        ON st_intersects(st_centroid(tract.geom), st_transform(nvi.geom, 4326))
    GROUP BY nvi.district_n, nvi.zone_id
)

SELECT
    acs.district_n AS council_districts,
    acs.zone_id,
    coalesce(cr.ped_crash_count, 0) AS total_ped_crash,
    coalesce(acs.total_pop, 1) AS total_population,
    (
        coalesce(cr.ped_crash_count, 0) * 10000.0 / nullif(acs.total_pop, 0)
    ) AS crash_per_10000
FROM zone_population AS acs
LEFT JOIN crash AS cr
    ON acs.district_n = cr.district_n AND acs.zone_id = cr.zone_id
ORDER BY acs.district_n, acs.zone_id;
