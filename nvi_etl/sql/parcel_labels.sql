SELECT d3_id, zone_id, district_number
FROM master
JOIN nvi.neighborhood_zones zones
    ON st_within(st_transform(st_centroid(master.geom), 2898), zones.geometry)
WHERE active;