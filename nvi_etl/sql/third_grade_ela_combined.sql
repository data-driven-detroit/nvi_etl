WITH base AS (
    SELECT
        sch.building_code,
        sch.geometry,
        e.number_assessed,
        e.total_met
    FROM education.eem_geocoded sch
    JOIN education.assessments e
        ON sch.building_code = e.building_code
    WHERE sch.start_date::DATE =:start_date
      AND e.start_date::DATE   =:start_date
      AND e.building_code <> '00000'
      AND e.grade_content_tested = 3
      AND e.subject = 'ELA'
),

-- city boundary schools
city_schools AS (
    SELECT b.*
    FROM base b
    JOIN nvi.city_boundary cb
      ON st_intersects(
            cb.geometry,
            st_transform(b.geometry, 2898)
         )
),

-- council district schools
cd_schools AS (
    SELECT
        b.*,
        cd.district_number AS geography
    FROM base b
    JOIN nvi.detroit_council_districts cd
      ON st_intersects(
            cd.geometry,
            st_transform(b.geometry, 2898)
         )
     AND cd.start_date::DATE = :geometry_start_date
),

-- neighborhood zone schools
zone_schools AS (
    SELECT
        b.*,
        z.zone_id AS geography
    FROM base b
    JOIN nvi.neighborhood_zones z
      ON st_intersects(
            z.geometry,
            st_transform(b.geometry, 2898)
         )
     AND z.start_date::DATE = :geometry_start_date
)

--combined result

SELECT
    'citywide' AS geo_type,
    'citywide' AS geography,
    SUM(number_assessed) AS universe_g3_ela,
    SUM(total_met)       AS count_g3_ela,
    SUM(total_met) / SUM(number_assessed) AS percentage_g3_ela,
    :year AS year
FROM city_schools

UNION ALL

SELECT
    'council_districts' AS geo_type,
    geography,
    SUM(number_assessed) AS universe_g3_ela,
    SUM(total_met)       AS count_g3_ela,
    SUM(total_met) / SUM(number_assessed) AS percentage_g3_ela,
    :year AS year
FROM cd_schools
GROUP BY geography

UNION ALL

SELECT
    'neighborhood_zones' AS geo_type,
    geography,
    SUM(number_assessed) AS universe_g3_ela,
    SUM(total_met)       AS count_g3_ela,
    SUM(total_met) / SUM(number_assessed) AS percentage_g3_ela,
    :year AS year
FROM zone_schools
GROUP BY geography;
