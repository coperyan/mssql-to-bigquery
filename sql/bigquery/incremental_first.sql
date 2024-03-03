BEGIN

    --Insert all new records
    CREATE OR REPLACE TABLE `{dataset}.{table}` AS 
    SELECT *
    FROM `{dataset}.stg_{table}`;

    --Delete staging table
    DROP TABLE `{dataset}.stg_{table}`;


END;