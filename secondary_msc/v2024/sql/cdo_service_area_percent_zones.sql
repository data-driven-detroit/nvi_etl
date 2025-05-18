WITH coverage AS (
    -- Combine all geographies into a single shape
    SELECT st_union(geom) AS geom
    FROM shp.becdd_47cdoserviceareas_20220815
),
zones AS (
    SELECT *
    FROM nvi.neighborhood_zones
    WHERE start_date = DATE '2026-01-01'
)
SELECT
    'zone' AS geo_type,
    zones.zone_id AS geography,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(zones.geometry, 4326), cov.geom))
    / ST_AREA(ST_TRANSFORM(zones.geometry, 4326)) AS percentage_cdo_coverage
FROM zones
CROSS JOIN coverage cov;  -- JOIN the combined shape to every for calc
