SELECT location.id AS location_id,
       3 AS indicator_id,
       NULL AS filter_option_id,
       2 AS source_id,
       start_date,
       end_date, -- end_date
       count, -- count
       NULL universe, 
       NULL AS percentage,
       count / (st_area(st_transform(location.geometry, 2898)) / 5280^2) AS rate,
       1 AS rate_per, -- rate_per
       NULL AS dollars,
       NULL AS index
FROM location
INNER JOIN context_value ON location.id = context_value.location_id
    WHERE end_date = DATE '2024-12-31'
        AND location_type_id != '4'
            AND indicator_id = '1';