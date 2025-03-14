--- Set to Data: Public
--- City of Detroit
with school as (SELECT district_code, district_name, building_code, building_name
FROM education.eem_schools as sch
JOIN shp.detroit_city_boundary_01182023 cb
ON st_intersects(sch.geometry, cb.geom)
WHERE sch.start_date >= '2023-07-01' AND sch.end_date <= '2024-06-29')

SELECT sum(number_assessed) as number_assessed_2023, sum(total_met) as met_ela_2023, (sum(total_met) / sum(number_assessed)) * 100  AS percentage_met_ela_2023
FROM school as s
join education.g3_ela_school as e
on s.building_code = e.building_code
WHERE year = '2023';

--- CD 2026
with school as (SELECT district_code, district_name, building_code, building_name, council_di
FROM education.eem_schools as sch
JOIN shp."Detroit_City_Council_Districts_2026" cb
ON st_intersects(st_transform(cb.geom,4326),sch.geometry)
WHERE sch.start_date >= '2023-07-01' AND sch.end_date <= '2024-06-29')

SELECT s.council_di as council_districts, sum(number_assessed) as number_assessed_2023, sum(total_met) as met_ela_2023, (sum(total_met) / sum(number_assessed)) * 100  AS percentage_met_ela_2023
FROM school as s
join education.g3_ela_school as e
on s.building_code = e.building_code
WHERE year = '2023'
GROUP BY s.council_di;

--- NVI Zones
with school as (SELECT district_code, district_name, building_code, building_name, cb.district_n, cb.zone_id
FROM education.eem_schools as sch
JOIN shp.nvi_neighborhood_zones_temp_2025 cb
ON st_intersects(sch.geometry, st_transform(cb.geom,4326))
WHERE sch.start_date >= '2023-07-01' AND sch.end_date <= '2024-06-29')

SELECT s.district_n as district_number, s.zone_id as zone_id, sum(number_assessed) as number_assessed_2023, sum(total_met) as met_ela_2023, (sum(total_met) / sum(number_assessed)) * 100  AS percentage_met_ela_2023
FROM school as s
join education.g3_ela_school as e
on s.building_code = e.building_code
WHERE year = '2023'
GROUP BY s.district_n, s.zone_id;
