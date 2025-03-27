WITH coverage AS (
    -- Combine all geographies into a single shape
    SELECT st_union(geom) AS geom
    FROM shp.becdd_47cdoserviceareas_20220815
),
counts AS (
    SELECT 
        zone_id, 
        count(*) AS num_cdos
    FROM nvi.neighborhood_zones zones
    JOIN shp.becdd_47cdoserviceareas_20220815 orgs
        ON ST_INTERSECTS(ST_TRANSFORM(zones.geometry, 4326), orgs.geom)
    GROUP BY zone_id
)
SELECT
    'neighborhood_zones' AS geo_type,
    zones.zone_id AS geography,
    counts.num_cdos,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(zones.geometry, 4326), cov.geom)) * 100
    / ST_AREA(ST_TRANSFORM(zones.geometry, 4326)) AS pct_cdo_coverage
FROM nvi.neighborhood_zones zones
    JOIN coverage cov ON TRUE  -- JOIN the combined shape to every for calc
JOIN counts 
    ON counts.zone_id = zones.zone_id;
