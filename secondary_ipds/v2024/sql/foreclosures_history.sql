WITH labeled_foreclosures AS (
    SELECT
        year,
        zone_id,
        district_number
    FROM nvi.foreclosures AS fc
    INNER JOIN nvi.neighborhood_zones AS zones
        ON st_within(st_transform(fc.geom, 2898), zones.geometry)
    WHERE fc.year > 2010
)

SELECT
    'citywide' AS geo_type,
    'Detroit' AS geography,
    year,
    count(*) AS count_tax_foreclosures
FROM labeled_foreclosures
GROUP BY year
UNION DISTINCT
SELECT
    'district' AS geo_type,
    district_number::text AS geography,
    year,
    count(*) AS count_tax_foreclosures
FROM labeled_foreclosures
GROUP BY year, district_number
UNION DISTINCT
SELECT
    'zone' AS geo_type,
    zone_id AS geography,
    year,
    count(*) AS count_tax_foreclosures
FROM labeled_foreclosures
GROUP BY year, zone_id;
