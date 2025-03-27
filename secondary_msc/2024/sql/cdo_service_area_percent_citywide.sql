WITH coverage AS (
    -- Combine all geographies into a single shape
    SELECT st_union(geom) AS geom
    FROM shp.becdd_47cdoserviceareas_20220815
),
counts AS (
    SELECT 
        count(*) AS num_cdos
    FROM nvi.city_boundary city
    JOIN shp.becdd_47cdoserviceareas_20220815 orgs
        ON ST_INTERSECTS(ST_TRANSFORM(city.geometry, 4326), orgs.geom)
)
SELECT
    'citywide' AS geo_type,
    'citywide' AS geography,
    counts.num_cdos,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(city.geometry, 4326), cov.geom)) * 100
    / ST_AREA(ST_TRANSFORM(city.geometry, 4326)) AS pct_cdo_coverage
FROM nvi.city_boundary city
    JOIN coverage cov ON TRUE  -- JOIN the combined shape to every for calc
JOIN counts ON TRUE;
