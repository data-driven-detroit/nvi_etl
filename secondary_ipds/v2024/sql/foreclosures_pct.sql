WITH flagged_parcels AS (SELECT pc.parcel_id,
                                pc.geom,
                                fc.parcel_id IS NULL AS not_in_foreclosure
                         FROM raw.detodp_assessors_20260131 AS pc
                                  LEFT JOIN raw.tax_foreclosures_2025 AS fc
                                            ON fc.parcel_id = pc.parcel_id),

     parcels_with_zones AS (SELECT fp.parcel_id,
                                   fp.not_in_foreclosure,
                                   zones.zone_id
                            FROM flagged_parcels AS fp
                                     JOIN nvi.neighborhood_zones AS zones
                                          ON ST_INTERSECTS(
                                                  ST_CENTROID(
                                                          ST_TRANSFORM(fp.geom::geometry, 2898)), zones.geometry)),

     parcels_with_districts AS (SELECT fp.parcel_id,
                                       fp.not_in_foreclosure,
                                       districts.district_number
                                FROM flagged_parcels AS fp
                                         JOIN nvi.detroit_council_districts AS districts
                                              ON ST_INTERSECTS(
                                                      ST_CENTROID(
                                                              ST_TRANSFORM(fp.geom::geometry, 2898)),
                                                      districts.geometry)),

     citywide AS (SELECT 'citywide'                                                             AS geo_type,
                         'Detroit'                                                             AS geography,
                         COUNT(*) FILTER (WHERE not_in_foreclosure)                             AS count_non_foreclosures,
                         COUNT(*) FILTER (WHERE NOT not_in_foreclosure)                         AS count_foreclosures,
                         COUNT(*)                                                               AS universe_non_foreclosures,
                         COUNT(*)                                                               AS universe_foreclosures,
                         100.0 * COUNT(*) FILTER (WHERE not_in_foreclosure)::NUMERIC / COUNT(*) AS percentage_non_foreclosures
                  FROM flagged_parcels),

     by_district AS (SELECT 'district'                                                            AS geo_type,
                            district_number::TEXT                                                  AS geography,
                            COUNT(*) FILTER (WHERE not_in_foreclosure)                             AS count_non_foreclosures,
                            COUNT(*) FILTER (WHERE NOT not_in_foreclosure)                         AS count_foreclosures,
                            COUNT(*)                                                               AS universe_non_foreclosures,
                            COUNT(*)                                                               AS universe_foreclosures,
                            100.0 * COUNT(*) FILTER (WHERE not_in_foreclosure)::NUMERIC / COUNT(*) AS percentage_non_foreclosures
                     FROM parcels_with_districts
                     GROUP BY district_number),

     by_zone AS (SELECT 'zone'                                                                 AS geo_type,
                        zone_id::TEXT                                                          AS geography,
                        COUNT(*) FILTER (WHERE not_in_foreclosure)                             AS count_non_foreclosures,
                        COUNT(*) FILTER (WHERE NOT not_in_foreclosure)                         AS count_foreclosures,
                        COUNT(*)                                                               AS universe_non_foreclosures,
                        COUNT(*)                                                               AS universe_foreclosures,
                        100.0 * COUNT(*) FILTER (WHERE not_in_foreclosure)::NUMERIC / COUNT(*) AS percentage_non_foreclosures
                 FROM parcels_with_zones
                 GROUP BY zone_id)

SELECT *, 2025 AS year
FROM citywide

UNION ALL

SELECT *, 2025 AS year
FROM by_district

UNION ALL

SELECT *, 2025 AS year
FROM by_zone;



