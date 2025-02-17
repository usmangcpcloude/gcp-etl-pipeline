-- INSERT INTO `ingestion-framework-450909.dd_curated.tehsil_hlp`
--     (tehsil_key, tehsil_name, source, ins_tmstmp, upd_tmstmp, table_id, batch_id, oper)
-- SELECT DISTINCT 
--     max_tehsil_key + ROW_NUMBER() OVER() AS tehsil_key,
--     LOWER(a.tehsil) AS tehsil_name, 
--     'punjab_census' AS source,
--     CURRENT_TIMESTAMP() AS ins_tmstmp, 
--     CURRENT_TIMESTAMP() AS upd_tmstmp, 
--     999 as table_id, 
--     'batch_id' AS batch_id,  -- Ensure batch_id is passed correctly
--     'I' AS oper
-- FROM `ingestion-framework-450909.dd_raw.population` a
-- LEFT JOIN `ingestion-framework-450909.dd_curated.tehsil_hlp` HLP
--     ON LOWER(REPLACE(TRIM(a.tehsil), ' ', '')) = LOWER(REPLACE(TRIM(HLP.tehsil_name), ' ', ''))
-- CROSS JOIN (
--     SELECT COALESCE(MAX(tehsil_key), 0) AS max_tehsil_key
--     FROM `ingestion-framework-450909.dd_curated.tehsil_hlp`
-- ) tehsil_key_max
-- WHERE HLP.tehsil_key IS NULL;

INSERT INTO `{project}.{env}_curated.tehsil_hlp`
    (tehsil_key, tehsil_name, source, ins_tmstmp, upd_tmstmp, table_id, batch_id, oper)
    SELECT DISTINCT 
        max_tehsil_key + ROW_NUMBER() OVER() AS tehsil_key,
        LOWER(a.tehsil) AS tehsil_name, 
        'punjab_census' AS source,
        CURRENT_TIMESTAMP() AS ins_tmstmp, 
        CURRENT_TIMESTAMP() AS upd_tmstmp, 
        999 as table_id, 
        '{batch_id}' AS batch_id,  -- Ensure batch_id is passed correctly
        'I' AS oper
    FROM `{project}.{env}_raw.population` a
    LEFT JOIN `{project}.{env}_curated.tehsil_hlp` HLP
        ON LOWER(REPLACE(TRIM(a.tehsil), ' ', '')) = LOWER(REPLACE(TRIM(HLP.tehsil_name), ' ', ''))
    CROSS JOIN (
        SELECT COALESCE(MAX(tehsil_key), 0) AS max_tehsil_key
        FROM `{project}.{env}_curated.tehsil_hlp`
    ) tehsil_key_max
    WHERE HLP.tehsil_key IS NULL;