--- Set to Data - Public
--- CDO SERVICE AREA
--- FOR NVI ZONES
select *
from shp.becdd_47cdoserviceareas_20220815
-- trying to get the percent of land area covered by the cdo
select nvi.*, ST_AREA(ST_INTERSECTION(st_transform(nvi.geom,4326), cdo.geom)) / ST_AREA(st_transform(nvi.geom,4326)) as pcover
from shp.nvi_neighborhood_zones_temp_2025 nvi
left join shp.becdd_47cdoserviceareas_20220815 cdo on st_intersects(st_transform(nvi.geom,4326), cdo.geom)

select orgname, count(*)
from (
select *
from shp.nvi_neighborhood_zones_temp_2025 nvi
left join shp.becdd_47cdoserviceareas_20220815 cdo on st_intersects(st_transform(nvi.geom,4326), cdo.geom)) q1
group by orgname

--- CD 2026
select *
from shp.becdd_47cdoserviceareas_20220815
-- trying to get the percent of land area covered by the cdo
select cd.*, ST_AREA(ST_INTERSECTION(st_transform(cd.geom,4326), cdo.geom)) / ST_AREA(st_transform(cd.geom,4326)) as pcover
from shp."Detroit_City_Council_Districts_2026" cd
left join shp.becdd_47cdoserviceareas_20220815 cdo on st_intersects(st_transform(cd.geom,4326), cdo.geom)

select orgname, count(*)
from (
select *
from shp."Detroit_City_Council_Districts_2026" cd
left join shp.becdd_47cdoserviceareas_20220815 cdo on st_intersects(st_transform(cd.geom,4326), cdo.geom)) q1
group by orgname


--- City of Detroit
select *
from shp.becdd_47cdoserviceareas_20220815
-- trying to get the percent of land area covered by the cdo
select cd.*, ST_AREA(ST_INTERSECTION(st_transform(cd.geom,4326), cdo.geom)) / ST_AREA(st_transform(cd.geom,4326)) as pcover
from shp.detroit_city_boundary_01182023 cd
left join shp.becdd_47cdoserviceareas_20220815 cdo on st_intersects(st_transform(cd.geom,4326), cdo.geom)

select orgname, count(*)
from (
select *
from shp.detroit_city_boundary_01182023 cd
left join shp.becdd_47cdoserviceareas_20220815 cdo on st_intersects(st_transform(cd.geom,4326), cdo.geom)) q1
group by orgname
