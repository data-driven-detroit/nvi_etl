--- Council District 2026
WITH building_permits AS (
    SELECT
        nvi.council_di,
        COUNT(*) AS building_permit_count
    FROM detodp_building_permits AS dp
    INNER JOIN shp."Detroit_City_Council_Districts_2026" AS nvi
        ON
            ST_INTERSECTS(
                ST_TRANSFORM(nvi.geom, 4326),
                ST_SETSRID(ST_POINT(dp.lon, dp.lat), 4326)
            )
    GROUP BY nvi.council_di
),

zone_population AS (
    SELECT
        nvi.council_di,
        SUM(b01003001) AS total_pop
    FROM public.b01003_moe AS acs
    INNER JOIN shp.tiger_census_2020_tract_mi AS tract
        ON RIGHT(acs.geoid, 11) = tract.geoid
    INNER JOIN shp."Detroit_City_Council_Districts_2026" AS nvi
        -- Census tract population assigned to council districts
        ON ST_INTERSECTS(ST_CENTROID(tract.geom), ST_TRANSFORM(nvi.geom, 4326))
    GROUP BY nvi.council_di
)

SELECT
    acs.council_di AS council_districts,
    COALESCE(rms.building_permit_count, 0) AS total_building_permits,
    COALESCE(acs.total_pop, 1) AS total_population,
    (
        COALESCE(rms.building_permit_count, 0)
        * 10000.0
        / NULLIF(acs.total_pop, 0)
    ) AS building_permits_per_10000
FROM zone_population AS acs
LEFT JOIN building_permits AS rms
    ON acs.council_di = rms.council_di
ORDER BY acs.council_di;

