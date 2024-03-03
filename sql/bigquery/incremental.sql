BEGIN

    --Delete records that already exist & have been updated
    DELETE FROM `{dataset}.{table}`
    WHERE ({uuid}) in (
        SELECT ({uuid})
        FROM `{dataset}.stg_{table}`
    );

    --Insert all new records
    INSERT INTO `{dataset}.{table}`
    SELECT *
    FROM `{dataset}.stg_{table}`;

    --Delete staging table
    DROP TABLE `{dataset}.stg_{table}`;


END;