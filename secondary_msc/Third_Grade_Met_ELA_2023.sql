--- Set to Data: Public
with school as (SELECT district_code, district_name, building_code, building_name
FROM education.eem_schools as sch
JOIN shp.detroit_city_boundary_01182023 cb
ON st_within(sch.geometry, cb.geom)
WHERE sch.start_date >= '2023-07-01' AND sch.end_date <= '2024-06-29')

SELECT sum(number_assessed) as number_assessed_2023, sum(total_met) as met_ela_2023, (sum(total_met) / sum(number_assessed)) * 100  AS percentage_met_ela_2023
FROM school as s
join education.g3_ela_school as e
on s.building_code = e.building_code
WHERE year = '2023';