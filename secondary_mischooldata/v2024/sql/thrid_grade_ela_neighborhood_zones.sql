--- NVI Zones
WITH school AS (
    SELECT district_code, 
           --district_name, 
           building_code, 
           building_name, 
           zones.zone_id
    FROM education.eem_geocoded as sch
    JOIN nvi.neighborhood_zones zones
        ON st_intersects(zones.geometry, st_transform(sch.geometry, 2898))
    WHERE sch.start_date = '2023-07-01'
    AND zones.start_date = '2026-01-01'
)
SELECT 'neighborhood_zones' AS geo_type,
       s.zone_id as geography, 
       sum(number_assessed) AS universe_g3_ela, 
       sum(total_met) AS count_g3_ela, 
       sum(total_met) / sum(number_assessed) AS percentage_g3_ela,
       2024 AS year -- FIXME: THIS NEEDS TO BE CHANGED
FROM school AS s
JOIN education.assessments AS e
    ON s.building_code = e.building_code
WHERE
    e.start_date = '2023-07-01'
    AND e.building_code <> '00000' 
    AND e.grade_content_tested = 3 
    AND e.subject = 'ELA'
GROUP BY s.zone_id, year;
