--- Set to Data - Public
--- CDO SERVICE AREA
--- FOR NVI ZONES
select *
from shp.becdd_47cdoserviceareas_20220815;

-- trying to get the percent of land area covered by the cdo
select
    nvi.*,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(nvi.geom, 4326), cdo.geom))
    / ST_AREA(ST_TRANSFORM(nvi.geom, 4326)) as pcover
from shp.nvi_neighborhood_zones_temp_2025 as nvi
left join
    shp.becdd_47cdoserviceareas_20220815 as cdo
    on ST_INTERSECTS(ST_TRANSFORM(nvi.geom, 4326), cdo.geom);

select
    orgname,
    COUNT(*)
from (
    select *
    from shp.nvi_neighborhood_zones_temp_2025 as nvi
    left join
        shp.becdd_47cdoserviceareas_20220815 as cdo
        on ST_INTERSECTS(ST_TRANSFORM(nvi.geom, 4326), cdo.geom)
) as q1
group by orgname;
