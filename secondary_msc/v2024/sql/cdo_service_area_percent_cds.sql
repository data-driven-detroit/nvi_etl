WITH coverage AS (
    -- Combine all geographies into a single shape
    SELECT st_union(geom) AS geom
    FROM shp.becdd_47cdoserviceareas_20220815
),
detroit_council_districts AS (
        SELECT
            *
        FROM
            nvi.detroit_council_districts
        WHERE
            start_date = DATE '2026-01-01'
)
SELECT
    'district' AS geo_type,
    cd.district_number AS geography,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(cd.geometry, 4326), cov.geom))
    / ST_AREA(ST_TRANSFORM(cd.geometry, 4326)) AS percentage_cdo_coverage
FROM detroit_council_districts cd
CROSS JOIN coverage cov;  -- JOIN the combined shape to every for calc