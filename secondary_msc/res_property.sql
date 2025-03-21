-- percent of non poor conditioned residential parcels
create table msc.nvi_prop_conditions_2025 as
-- 1. get the total residential parcels with structures
with base as (
    select ases.geom,
           ases.parcel_num,
           det.d3_id,
           case when bf.d3_id is not null then 1 else 0 end as structure,
           likely_res
    from raw.detodp_assessor_20240205 ases
             left join raw.detodp_assessor_20240205_det det on ases.id = det.id
             left join building_file_20230313_2 bf on bf.d3_id = det.d3_id
             left join a_vacancy_base_val_res rs on rs.d3_id = bf.d3_id)
select base.*,
       bli.blight,
       mcm.condition,
       vacant
from base
-- 2. flag parcels with blight tickets in the past 2 years
         left join (
    select d3_id, count(*) as blight
    from raw.detodp_blight_violations_20250204
    where extract (year from violation_date::date) > 2021 and d3_id is not null
    group by d3_id
) bli on base.d3_id = bli.d3_id
-- 3. flag mcm demo parcels
         left join (
    SELECT d3_id, condition
    from raw.survey_mcm_2014 mcm
    where d3_id is not null and mcm.condition ilike '%demo%'
) mcm on mcm.d3_id = base.d3_id
-- 4. flag parcel vacant since 2022
         left join (
    select d3_id,
           case when percent_occupied + percent_occupied_2 + percent_occupied_3 +
                     percent_occupied_4 + percent_occupied_5 + percent_occupied_6 = 0 then 1 else 0 end as vacant
    from (
             with t1 as (SELECT d3_id, round((1.0-avg(c12::bool::int::numeric))*100.0,1) percent_occupied
                         FROM raw.valassis_vnefplus_mi_20241017_det
                         where d3_id is not null and c12 is not null
                         GROUP BY d3_id)
             select t1.*,
                    percent_occupied_2,
                    percent_occupied_3,
                    percent_occupied_4,
                    percent_occupied_5,
                    percent_occupied_6,
                    percent_occupied_7,
                    percent_occupied_8
             from t1
                      join
                  (SELECT d3_id, round((1.0-avg(c12::bool::int::numeric))*100.0,1) percent_occupied_2
                   FROM raw.valassis_vnefplus_mi_20240711_det
                   where d3_id is not null and c12 is not null
                   GROUP BY d3_id) t2 on t1.d3_id = t2.d3_id
                      join
                  (SELECT d3_id, round((1.0-avg(c12::bool::int::numeric))*100.0,1) percent_occupied_3
                   FROM raw.valassis_vnefplus_mi_20240411_det
                   where d3_id is not null and c12 is not null
                   GROUP BY d3_id) t3 on t1.d3_id = t3.d3_id
                      join
                  (SELECT d3_id, round((1.0-avg(c12::bool::int::numeric))*100.0,1) percent_occupied_4
                   FROM raw.valassis_vnefplus_mi_20240116_det
                   where d3_id is not null and c12 is not null
                   GROUP BY d3_id) t4 on t1.d3_id = t4.d3_id
                      join
                  (SELECT d3_id, round((1.0-avg(c12::bool::int::numeric))*100.0,1) percent_occupied_5
                   FROM raw.valassis_vnefplus_mi_20231017_det
                   where d3_id is not null and c12 is not null
                   GROUP BY d3_id) t5 on t1.d3_id = t5.d3_id
                      join
                  (SELECT d3_id, round((1.0-avg(c12::bool::int::numeric))*100.0,1) percent_occupied_6
                   FROM raw.valassis_vnefplus_mi_20230807_det
                   where d3_id is not null and c12 is not null
                   GROUP BY d3_id) t6 on t1.d3_id = t6.d3_id
                        join
                   (SELECT d3_id, round((1.0-avg(c12::bool::int::numeric))*100.0,1) percent_occupied_7
                   FROM raw.valassis_vnefplus_mi_20230425_det
                   where d3_id is not null and c12 is not null
                   GROUP BY d3_id) t7 on t1.d3_id = t7.d3_id
                      join
                   (SELECT d3_id, round((1.0-avg(c12::bool::int::numeric))*100.0,1) percent_occupied_8
                   FROM raw.valassis_vnefplus_mi_20230123_det
                   where d3_id is not null and c12 is not null
                   GROUP BY d3_id) t8 on t1.d3_id = t8.d3_id) occupancy
) vac on base.d3_id = vac.d3_id
-- Finally add filter back in
where likely_res = 1 and structure = 1

--- NVI
select  nvi.district_n, nvi.zone_id,
       sum(non_blight) num_non_blight,
       count(q1.geom) num_residential_parcels,
       (sum(non_blight) * 100 / count(q1.geom))  AS percentage_res_2025
from shp.nvi_neighborhood_zones_temp_2025 nvi
left join (
select geom,
       case when blight is null and condition is null and vacant = 0 then 1 else 0 end as non_blight
from msc.nvi_prop_conditions_2025) q1 on st_intersects(st_transform(nvi.geom,4326), q1.geom)
group by nvi.district_n, nvi.zone_id

--- City if Detroit
select sum(non_blight) num_non_blight,
       count(q1.geom) num_residential_parcels,
       (sum(non_blight)  * 100 / count(q1.geom)) AS percentage_res_2025
from shp.detroit_city_boundary_01182023 det
left join (
select geom,
       case when blight is null and condition is null and vacant = 0 then 1 else 0 end as non_blight
from msc.nvi_prop_conditions_2025) q1 on st_intersects(st_transform(det.geom,4326), q1.geom)

--- CD 2026
select  council_di,
        sum(non_blight) num_non_blight,
       count(q1.geom) num_residential_parcels,
       (sum(non_blight) * 100 / count(q1.geom))  AS percentage_res_2025
from shp.detroit_city_council_districts_2026 det
left join (
select geom,
       case when blight is null and condition is null and vacant = 0 then 1 else 0 end as non_blight
from msc.nvi_prop_conditions_2025) q1 on st_intersects(st_transform(det.geom,4326), q1.geom)
group by council_di