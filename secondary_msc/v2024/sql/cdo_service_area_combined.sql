WITH coverage AS (
    SELECT st_union(geom) AS geom
    FROM {cdo_table}
),
detroit_council_districts AS (
    SELECT *
    FROM nvi.detroit_council_districts
    WHERE start_date = :geom_date
),
zones AS (
    SELECT *
    FROM nvi.neighborhood_zones
    WHERE start_date = :geom_date
)
SELECT
    'citywide' AS geo_type,
    'Detroit' AS geography,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(city.geometry, 4326), cov.geom))
        / ST_AREA(ST_TRANSFORM(city.geometry, 4326)) AS percentage_cdo_coverage
FROM nvi.city_boundary city
CROSS JOIN coverage cov

UNION ALL

SELECT
    'district' AS geo_type,
    cd.district_number AS geography,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(cd.geometry, 4326), cov.geom))
        / ST_AREA(ST_TRANSFORM(cd.geometry, 4326)) AS percentage_cdo_coverage
FROM detroit_council_districts cd
CROSS JOIN coverage cov

UNION ALL

SELECT
    'zone' AS geo_type,
    zones.zone_id AS geography,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(zones.geometry, 4326), cov.geom))
        / ST_AREA(ST_TRANSFORM(zones.geometry, 4326)) AS percentage_cdo_coverage
FROM zones
CROSS JOIN coverage cov
ORDER BY geo_type, geography;
