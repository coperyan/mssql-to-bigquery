# mssql-to-bigquery

Pipeline to ingest data from a MSSQL source, land it in Google Cloud Storage, then finally BigQuery. 

## Features

- Incremental Load
- Automatic conversion of data types

## Getting Started

### Dependencies

- google-cloud-bigquery
- google-cloud-storage
- pandas
- pyodbc

I'd also recommend installing [gsutil](https://cloud.google.com/storage/docs/gsutil) for authentication. 

### Environment Variables

Edit the `config.yml` file to provide your own environment variables as needed. 

- `gcp_project` - Google Cloud Platform - Project Name
- `gcp_dataset` - BigQuery Dataset (destination)
- `gcs_bucket` - Google Cloud Storage Bucket
- `gcs_prefix` - Path in Storage Bucket where project will create root
- `mssql_hostname` - SQL Server Hostname
- `mssql_username` - Login User (Optional)
- `mssql_password` - Login Password (Optional)

You can also skip usage of this file and set the variables independently. 

## Configuration

For each extract, you must define some parameters passed to the `MSSQLtoBigQuery` class when initialized: 

Required
- `ingestion_type`
  - `full_replace`
  - `append` - use a column (UUID or created date) to only ingest new records
  - `incremental` - similar to append, will ingest new AND records that have changed
- `database`
- `schema`
- `mssql_object_name` - view or table you want to process

Optional
- `bq_table_name` - destination table name if different than source
- `chunks` - rows per file iteration
- `keys` - str or list of str - fields that represent unique constraint
- `sql_query` - path to .sql file used to define custom query
- `last_val_column` - column used to limit records for append/incremental loads
- `last_val_query` - path to .sql file used to define custom last val
- `last_val_default` - fallback value to be used if table does not already exist in destination
- `incremental_keys` - list of columns to match when comparing staging to existing records
  
## Process

### Initialize MSSQL Connection

- Based on the environment variables provided, a pyodbc connection will be established
- Auth will be using a trusted connection if either user or password variables are missing.

### Check Last Value

- If the ingestion_type is `append` or `incremental` - this step will be executed
- This function identifies the maximum value (string, numeric, date, etc.) in the `last_val_column`
- When the query to extract data from MSSQL is built, it will only be pulling records with a value greater than this
- Custom queries can be used via the `last_val_query`

### Build MSSQL Query



### Build Schema JSON

### Get Data

### Load Data to GCS

### Load GCS to BQ

### Incremental Load

### Check Index (Keys)
