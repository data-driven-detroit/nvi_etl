SELECT
    i.id AS indicator_id,
    i.name AS indicator,
    l.id AS location_id,
    l.name AS location,
    count,
    universe,
    rate,
    rate_per,
    geometry
FROM value AS v
INNER JOIN indicator AS i
    ON v.indicator_id = i.id
INNER JOIN location AS l
    ON v.location_id = l.id
WHERE
    year = 2024
    AND indicator_id = 30
ORDER BY location_id;
