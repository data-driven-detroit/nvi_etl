WITH annotated AS (
    SELECT pu.aggregate_to, zones.zone_id, zones.district_number, det.geom
    FROM raw.detodp_assessor_20240205 det
    JOIN nvi.neighborhood_zones zones
       ON ST_WITHIN(ST_TRANSFORM(det.geom, 2898), zones.geometry)
    JOIN nvi.parcel_use_summary pu
       ON det.use_code = pu.use_code
)
SELECT
    'Detroit' AS geography,
    'citywide' AS geo_type,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Institutional') AS count_institutional,
    SUM(ST_AREA(annotated.geom))                                               AS universe_institutional,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Empty Lot')     AS count_empty_lot,
    SUM(ST_AREA(annotated.geom))                                               AS universe_empty_lot,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Other')         AS count_other,
    SUM(ST_AREA(annotated.geom))                                               AS universe_other,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Residential')   AS count_residential,
    SUM(ST_AREA(annotated.geom))                                               AS universe_residential,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Industrial')    AS count_industrial,
    SUM(ST_AREA(annotated.geom))                                               AS universe_industrial,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Commercial')    AS count_commercial,
    SUM(ST_AREA(annotated.geom))                                               AS universe_commercial
FROM annotated

UNION DISTINCT

SELECT
    zone_id AS geography,
    'zone' AS geo_type,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Institutional') AS count_institutional,
    SUM(ST_AREA(annotated.geom))                                               AS universe_institutional,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Empty Lot')     AS count_empty_lot,
    SUM(ST_AREA(annotated.geom))                                               AS universe_empty_lot,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Other')         AS count_other,
    SUM(ST_AREA(annotated.geom))                                               AS universe_other,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Residential')   AS count_residential,
    SUM(ST_AREA(annotated.geom))                                               AS universe_residential,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Industrial')    AS count_industrial,
    SUM(ST_AREA(annotated.geom))                                               AS universe_industrial,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Commercial')    AS count_commercial,
    SUM(ST_AREA(annotated.geom))                                               AS universe_commercial
FROM annotated
GROUP BY zone_id

UNION DISTINCT

SELECT
    district_number::text AS geography,
    'district' AS geo_type,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Institutional') AS count_institutional,
    SUM(ST_AREA(annotated.geom))                                               AS universe_institutional,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Empty Lot')     AS count_empty_lot,
    SUM(ST_AREA(annotated.geom))                                               AS universe_empty_lot,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Other')         AS count_other,
    SUM(ST_AREA(annotated.geom))                                               AS universe_other,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Residential')   AS count_residential,
    SUM(ST_AREA(annotated.geom))                                               AS universe_residential,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Industrial')    AS count_industrial,
    SUM(ST_AREA(annotated.geom))                                               AS universe_industrial,
    SUM(ST_AREA(annotated.geom)) FILTER (WHERE aggregate_to = 'Commercial')    AS count_commercial,
    SUM(ST_AREA(annotated.geom))                                               AS universe_commercial
FROM annotated
GROUP BY district_number;
