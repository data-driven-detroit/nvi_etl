WITH grade_union AS (
    SELECT
        st_union(geometry) AS geometry
    FROM nvi.holc_maps
    WHERE grade IN ('C', 'D')
)

SELECT
    'zone' as geo_type,
    zone_id as geography,
    st_area(st_intersection(zones.geometry, gu.geometry)) AS count_redlined,
    st_area(zones.geometry) AS universe_redlined,
    st_area(st_intersection(zones.geometry, gu.geometry))
    / st_area(zones.geometry) AS percentage_redlined
FROM nvi.neighborhood_zones AS zones
CROSS JOIN grade_union AS gu
WHERE start_date = DATE ' 2026-01-01'

UNION

SELECT
    'district' as geo_type,
    district_number as geography,
    st_area(st_intersection(districts.geometry, gu.geometry)) AS count_redlined,
    st_area(districts.geometry) AS universe_redlined,
    st_area(st_intersection(districts.geometry, gu.geometry))
    / st_area(districts.geometry) AS percentage_redlined
FROM nvi.detroit_council_districts AS districts
CROSS JOIN grade_union AS gu
WHERE start_date = DATE ' 2026-01-01'

UNION

SELECT
    'citywide' as geo_type,
    'Detroit' as geography,
    st_area(st_intersection(city.geometry, gu.geometry)) AS count_redlined,
    st_area(city.geometry) AS universe_redlined,
    st_area(st_intersection(city.geometry, gu.geometry))
    / st_area(city.geometry) AS percentage_redlined
FROM nvi.city_boundary AS city
CROSS JOIN grade_union AS gu;
