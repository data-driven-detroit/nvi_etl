WITH coverage AS (
    -- Combine all geographies into a single shape
    SELECT st_union(geom) AS geom
    FROM shp.becdd_47cdoserviceareas_20220815
),
WITH counts AS (
    SELECT district_number, count(*) AS num_cdos
    FROM nvi.detroit_council_districts cd
    JOIN shp.becdd_47cdoserviceareas_20220815 orgs
        ON ST_INTERSECTS(ST_TRANSFORM(cd.geom, orgs.geom)
    GROUP BY district_number;
)
SELECT
    'council_districts' AS geo_type,
    cd.district_number AS geography,
    counts.num_cdos,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(cd.geom, 4326), cov.geom)) * 100
    / ST_AREA(ST_TRANSFORM(cd.geom, 4326)) AS pct_cdo_coverage
FROM nvi.detroit_council_districts cd
    JOIN coverage cov ON TRUE  -- JOIN the combined shape to every for calc
JOIN counts 
    ON counts.district_number = cd.district_number;
