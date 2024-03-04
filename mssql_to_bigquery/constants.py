from google.cloud import bigquery

INGESTION_TYPES = {
    "append": {
        "incremental": False,
        "last_val": True,
        "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "write_type": bigquery.WriteDisposition.WRITE_APPEND,
    },
    "full_replace": {
        "incremental": False,
        "last_val": False,
        "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "write_type": bigquery.WriteDisposition.WRITE_TRUNCATE,
    },
    "incremental": {
        "incremental": True,
        "last_val": True,
        "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "write_type": bigquery.WriteDisposition.WRITE_TRUNCATE,
    },
}

TYPE_MAPPING = {
    "money": "FLOAT",
    "smallmoney": "FLOAT",
    "decimal": "FLOAT",
    "float": "FLOAT",
    "real": "FLOAT",
    "int": "INTEGER",
    "tinyint": "INTEGER",
    "smallint": "INTEGER",
    "bigint": "INTEGER",
    "datetime": "DATETIME",
    "smalldatetime": "DATETIME",
    "date": "DATE",
    "text": "STRING",
    "varchar": "STRING",
    "nvarchar": "STRING",
    "uniqueidentifier": "STRING",
    "nchar": "STRING",
    "char": "STRING",
    "bit": "BOOLEAN",
}
MSSQL_DRIVER = "ODBC Driver 17 for SQL Server"
