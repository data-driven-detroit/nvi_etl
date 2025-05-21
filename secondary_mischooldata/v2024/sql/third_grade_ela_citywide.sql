--- City of Detroit
WITH school AS (
    SELECT district_code, 
           district_name, 
           building_code, 
           building_name
    FROM education.eem_schools AS sch
    JOIN nvi.city_boundary cb
        ON st_intersects(cb.geometry, st_transform(sch.geometry, 2898))
    WHERE sch.start_date = '2023-07-01'
)
SELECT 'citywide' AS geo_type,
       'citywide' AS geography,
       sum(number_assessed) AS universe_g3_ela, 
       sum(total_met) AS count_g3_ela, 
       sum(total_met) / sum(number_assessed) AS percentage_g3_ela,
       2024 AS year -- FIXME: THIS NEEDS TO BE CHANGED
FROM school AS s
JOIN education.g3_ela_school AS e
    ON s.building_code = e.building_code
WHERE e.year = '2023';
