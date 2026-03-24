WITH grade_union AS (
    SELECT
        st_union(geometry) AS geometry
    FROM {redlining_table}
    WHERE grade IN ('C', 'D')
)

SELECT
    'zone' as geo_type,
    zone_id as geography,
    NULL AS count_redlined,
    NULL AS universe_redlined,
    100 * st_area(st_intersection(zones.geometry, gu.geometry))
    / st_area(zones.geometry) AS percentage_redlined
FROM nvi.neighborhood_zones AS zones
CROSS JOIN grade_union AS gu
WHERE start_date = :geom_date

UNION

SELECT
    'district' as geo_type,
    district_number as geography,
    NULL AS count_redlined,
    NULL AS universe_redlined,
    100 * st_area(st_intersection(districts.geometry, gu.geometry))
    / st_area(districts.geometry) AS percentage_redlined
FROM nvi.detroit_council_districts AS districts
CROSS JOIN grade_union AS gu
WHERE start_date = :geom_date

UNION

SELECT
    'citywide' as geo_type,
    'Detroit' as geography,
    NULL AS count_redlined,
    NULL AS universe_redlined,
    100 * st_area(st_intersection(city.geometry, gu.geometry))
    / st_area(city.geometry) AS percentage_redlined
FROM nvi.city_boundary AS city
CROSS JOIN grade_union AS gu;
