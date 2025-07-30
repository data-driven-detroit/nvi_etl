SELECT d3_id, filed_date
FROM out_tables.upw_flatfile_20230404
WHERE filed_date < DATE '2022-12-31';