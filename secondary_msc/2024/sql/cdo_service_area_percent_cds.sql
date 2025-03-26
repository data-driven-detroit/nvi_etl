--- CD 2026
SELECT *
FROM shp.becdd_47cdoserviceareas_20220815;

-- trying to get the percent of land area covered by the cdo
SELECT
    cd.*,
    ST_AREA(ST_INTERSECTION(ST_TRANSFORM(cd.geom, 4326), cdo.geom))
    / ST_AREA(ST_TRANSFORM(cd.geom, 4326)) AS pcover
FROM shp."Detroit_City_Council_Districts_2026" AS cd
LEFT JOIN
    shp.becdd_47cdoserviceareas_20220815 AS cdo
    ON ST_INTERSECTS(ST_TRANSFORM(cd.geom, 4326), cdo.geom);

SELECT
    orgname,
    COUNT(*)
FROM (
    SELECT *
    FROM shp."Detroit_City_Council_Districts_2026" AS cd
    LEFT JOIN
        shp.becdd_47cdoserviceareas_20220815 AS cdo
        ON ST_INTERSECTS(ST_TRANSFORM(cd.geom, 4326), cdo.geom)
) AS q1
GROUP BY orgname;
