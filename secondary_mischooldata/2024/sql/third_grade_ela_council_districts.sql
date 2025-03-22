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
SELECT s.council_di as council_districts, 
       sum(number_assessed) as number_assessed_2023, 
       sum(total_met) as met_ela_2023, 
       (sum(total_met) / sum(number_assessed)) * 100  AS percentage_met_ela_2023
FROM school AS s
JOIN education.g3_ela_school AS e
    ON s.building_code = e.building_code
WHERE year = '2023'
GROUP BY s.council_di;

