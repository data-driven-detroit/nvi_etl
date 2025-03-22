--- Set to Data: Public
--- City of Detroit
WITH school AS (
    SELECT district_code, 
           district_name, 
           building_code, 
           building_name
    FROM education.eem_schools AS sch
    JOIN shp.detroit_city_boundary_01182023 cb -- TODO Change this to the NVI shapefile for easier management
        ON st_intersects(sch.geometry, cb.geom)
    WHERE sch.start_date >= '2023-07-01' AND sch.end_date <= '2024-06-29'
)
SELECT sum(number_assessed) AS number_assessed_2023, 
       sum(total_met) AS met_ela_2023, 
       (sum(total_met) / sum(number_assessed)) * 100  AS percentage_met_ela_2023
FROM school AS s
JOIN education.g3_ela_school AS e
    ON s.building_code = e.building_code
WHERE year = '2023';

