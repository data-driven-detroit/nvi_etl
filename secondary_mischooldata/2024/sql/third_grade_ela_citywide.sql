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
SELECT 'citywide' AS geo_type,
       'citywide' AS geography,
       sum(number_assessed) AS g3_ela_number_assessed, 
       sum(total_met) AS g3_ela_total_met, 
       (sum(total_met) / sum(number_assessed)) * 100  AS g3_ela_pct_met,
       year
FROM school AS s
JOIN education.g3_ela_school AS e
    ON s.building_code = e.building_code
WHERE year = '2023'
GROUP BY year; -- TODO Once the g3_ela_school table is moved to 'start_date', 'end_date' this will break.
