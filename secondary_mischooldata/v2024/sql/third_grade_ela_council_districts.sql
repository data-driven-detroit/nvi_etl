--- CD 2026
WITH school AS (
    SELECT district_code, 
           district_name, 
           building_code, 
           building_name, 
           district_number
    FROM education.eem_schools as sch
    JOIN nvi.detroit_council_districts cd
        ON st_intersects(cd.geometry,st_transform(sch.geometry, 2898))
    WHERE sch.start_date = '2023-07-01'
    AND cd.start_date = DATE '2026-01-01'
)
SELECT 'council_districts' as geo_type,
       s.district_number as geography, 
       sum(number_assessed) AS universe_g3_ela, 
       sum(total_met) AS count_g3_ela, 
       sum(total_met) / sum(number_assessed) AS percentage_g3_ela,
       2024 AS year -- FIXME: THIS NEEDS TO BE CHANGED
FROM school AS s
JOIN education.g3_ela_school AS e
    ON s.building_code = e.building_code
WHERE e.year = '2023'
GROUP BY s.district_number, year;