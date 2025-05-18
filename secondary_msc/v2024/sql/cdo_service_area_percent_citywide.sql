WITH coverage AS (
    -- Combine all geographies into a single shape
    SELECT st_union(geom) AS geom
    FROM shp.becdd_47cdoserviceareas_20220815
)
SELECT
    'citywide' AS geo_type,
    'Detroit' AS geography,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(city.geometry, 4326), cov.geom))
    / ST_AREA(ST_TRANSFORM(city.geometry, 4326)) AS percentage_cdo_coverage
FROM nvi.city_boundary city
CROSS JOIN coverage cov;  -- JOIN the combined shape to every for calc
