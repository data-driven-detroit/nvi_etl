--- City of Detroit
select *
from shp.becdd_47cdoserviceareas_20220815;

-- trying to get the percent of land area covered by the cdo
select
    cd.*,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(cd.geom, 4326), cdo.geom))
    / ST_AREA(ST_TRANSFORM(cd.geom, 4326)) as pcover
from shp.detroit_city_boundary_01182023 as cd
left join
    shp.becdd_47cdoserviceareas_20220815 as cdo
    on ST_INTERSECTS(ST_TRANSFORM(cd.geom, 4326), cdo.geom);

select
    orgname,
    COUNT(*)
from (
    select *
    from shp.detroit_city_boundary_01182023 as cd
    left join
        shp.becdd_47cdoserviceareas_20220815 as cdo
        on ST_INTERSECTS(ST_TRANSFORM(cd.geom, 4326), cdo.geom)
) as q1
group by orgname;
