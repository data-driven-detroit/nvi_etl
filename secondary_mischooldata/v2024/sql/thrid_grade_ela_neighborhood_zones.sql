--- NVI Zones
WITH school AS (
    SELECT district_code, 
           district_name, 
           building_code, 
           building_name, 
           zones.zone_id
    FROM education.eem_schools as sch
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
JOIN education.g3_ela_school AS e
    ON s.building_code = e.building_code
WHERE e.year = '2023'
GROUP BY s.zone_id, year;
