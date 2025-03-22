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
group by council_di;
