-- percent of non poor conditioned residential parcels
CREATE TABLE IF NOT EXISTS msc.nvi_prop_conditions_2025 AS -- Don't create if the work has been done
-- 1. get the total residential parcels with structures
WITH base AS (
    SELECT ases.geom,
           ases.parcel_num,
           det.d3_id,
           CASE
                WHEN bf.d3_id IS NOT NULL THEN 1
                ELSE 0
            END AS STRUCTURE,
            likely_res
    FROM raw.detodp_assessor_20240205 ases
    LEFT JOIN raw.detodp_assessor_20240205_det det 
        ON ases.id = det.id
    LEFT JOIN building_file_20230313_2 bf 
        ON bf.d3_id = det.d3_id
    LEFT JOIN a_vacancy_base_val_res rs 
        ON rs.d3_id = bf.d3_id
)
SELECT base.*,
       bli.blight,
       mcm.condition,
       vacant
FROM base
-- 2. flag parcels with blight tickets in the past 2 years
LEFT JOIN (
    SELECT d3_id,
           count(*) AS blight
    FROM raw.detodp_blight_violations_20250204
    WHERE EXTRACT (YEAR FROM violation_date::date) > 2022
    AND d3_id IS NOT NULL
    GROUP BY d3_id
) bli ON base.d3_id = bli.d3_id
-- 3. flag mcm demo parcels
LEFT JOIN (
    SELECT d3_id,
           CONDITION
    FROM raw.survey_mcm_2014 mcm
    WHERE d3_id IS NOT NULL
    AND mcm.condition ilike '%demo%'
) mcm 
    ON mcm.d3_id = base.d3_id
-- 4. flag parcel vacant since 2022
LEFT JOIN (
	SELECT d3_id,
	       CASE WHEN (  percent_occupied 
                          + percent_occupied_2 
                          + percent_occupied_3 
                          + percent_occupied_4 
                          + percent_occupied_5 
                          + percent_occupied_6 ) = 0 
                    THEN 1
                    ELSE 0
	        END AS vacant
	FROM (
             WITH t1 AS (
		SELECT d3_id,
		       round((1.0-avg(c12::bool::int::NUMERIC))* 100.0, 1) percent_occupied
		FROM raw.valassis_vnefplus_mi_20241017_det
		WHERE d3_id IS NOT NULL
                AND c12 IS NOT NULL
		GROUP BY d3_id
             )
             SELECT t1.*,
                    percent_occupied_2,
                    percent_occupied_3,
                    percent_occupied_4,
                    percent_occupied_5,
                    percent_occupied_6,
                    percent_occupied_7,
                    percent_occupied_8
            FROM t1
		JOIN (
                    SELECT d3_id,
                           round((1.0-avg(c12::bool::int::NUMERIC))* 100.0, 1) percent_occupied_2
                    FROM raw.valassis_vnefplus_mi_20240711_det
                    WHERE d3_id IS NOT NULL
                    AND c12 IS NOT NULL
                    GROUP BY d3_id
                ) t2 ON t1.d3_id = t2.d3_id
		JOIN (
                    SELECT d3_id,
                           round((1.0-avg(c12::bool::int::NUMERIC))* 100.0, 1) percent_occupied_3
                    FROM raw.valassis_vnefplus_mi_20240411_det
                    WHERE d3_id IS NOT NULL
                    AND c12 IS NOT NULL
                    GROUP BY d3_id
                ) t3 ON t1.d3_id = t3.d3_id
		JOIN (
                    SELECT d3_id,
                           round((1.0-avg(c12::bool::int::NUMERIC))* 100.0, 1) percent_occupied_4
                    FROM raw.valassis_vnefplus_mi_20240116_det
                    WHERE d3_id IS NOT NULL
                          AND c12 IS NOT NULL
                    GROUP BY d3_id
                ) t4 ON t1.d3_id = t4.d3_id
		JOIN (
                    SELECT d3_id,
                           round((1.0-avg(c12::bool::int::NUMERIC))* 100.0, 1) percent_occupied_5
                    FROM raw.valassis_vnefplus_mi_20231017_det
                    WHERE d3_id IS NOT NULL
                    AND c12 IS NOT NULL
                    GROUP BY d3_id
                ) t5 ON t1.d3_id = t5.d3_id
		JOIN (
                    SELECT d3_id,
                           round((1.0-avg(c12::bool::int::NUMERIC))* 100.0,
                           1) percent_occupied_6
                    FROM raw.valassis_vnefplus_mi_20230807_det
                    WHERE d3_id IS NOT NULL
                    AND c12 IS NOT NULL
                    GROUP BY d3_id
                ) t6 ON t1.d3_id = t6.d3_id
		JOIN (
                    SELECT d3_id,
                           round((1.0-avg(c12::bool::int::NUMERIC))* 100.0, 1) percent_occupied_7
                    FROM raw.valassis_vnefplus_mi_20230425_det
                    WHERE d3_id IS NOT NULL
                    AND c12 IS NOT NULL
                    GROUP BY d3_id
                ) t7 ON t1.d3_id = t7.d3_id
		JOIN (
                    SELECT d3_id,
                           round((1.0-avg(c12::bool::int::NUMERIC))* 100.0,
                           1) percent_occupied_8
                    FROM raw.valassis_vnefplus_mi_20230123_det
                    WHERE d3_id IS NOT NULL
                    AND c12 IS NOT NULL
                    GROUP BY d3_id
                ) t8 ON t1.d3_id = t8.d3_id
            ) occupancy
) vac ON base.d3_id = vac.d3_id
-- Finally add filter back in
WHERE likely_res = 1
AND STRUCTURE = 1;
