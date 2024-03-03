SELECT
    data_type
FROM
    `{gcp_project}.{gcp_dataset}.{table_name}`
WHERE
    table_name = '{table_name}'
AND
    UPPER(field_name) = UPPER('{field_name}')
;