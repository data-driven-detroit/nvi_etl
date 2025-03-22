--- NVI Zones
WITH school AS (
    SELECT district_code, 
           district_name, 
           building_code, 
           building_name, 
           cb.district_n, 
           cb.zone_id
    FROM education.eem_schools as sch
    JOIN shp.nvi_neighborhood_zones_temp_2025 cb
        ON st_intersects(sch.geometry, st_transform(cb.geom,4326))
    WHERE sch.start_date >= '2023-07-01' AND sch.end_date <= '2024-06-29'
)
SELECT s.district_n as district_number, 
       s.zone_id as zone_id, 
       sum(number_assessed) as number_assessed_2023, 
       sum(total_met) as met_ela_2023, 
       (sum(total_met) / sum(number_assessed)) * 100  AS percentage_met_ela_2023
FROM school AS s
JOIN education.g3_ela_school AS e
    ON s.building_code = e.building_code
WHERE year = '2023'
GROUP BY s.district_n, s.zone_id;
