--- CD 2026
WITH school AS (
    SELECT district_code, 
           district_name, 
           building_code, 
           building_name, 
           council_di
    FROM education.eem_schools as sch
    JOIN shp."Detroit_City_Council_Districts_2026" cb
        ON st_intersects(st_transform(cb.geom,4326),sch.geometry)
    WHERE sch.start_date >= '2023-07-01' AND sch.end_date <= '2024-06-29'
)
SELECT 'council_districts' as geo_type,
       s.council_di as geography, 
       sum(number_assessed) AS universe_g3_ela, 
       sum(total_met) AS count_g3_ela, 
       sum(total_met) / sum(number_assessed) AS percentage_g3_ela,
       year
FROM school AS s
JOIN education.g3_ela_school AS e
    ON s.building_code = e.building_code
WHERE year = '2023'
GROUP BY s.council_di, year;

