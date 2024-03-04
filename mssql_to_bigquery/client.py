import os
import json

# import pyodbc
import logging
import decimal
import datetime
import pandas as pd
from typing import Union
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest

from mssql_to_bigquery.constants import (
    INGESTION_TYPES,
    MSSQL_DRIVER,
    TYPE_MAPPING,
)

logger = logging.getLogger(__name__)


def read_file(p, **kwargs) -> str:
    """Reads file and attempts to format_map with kwargs"""
    with open(p) as f:
        query = "".join([l for l in f])
    return query.format_map(kwargs)


def convert_type(v):
    """Converts values when writing to NL JSON"""
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.time)):
        return v.isoformat()
    return v


def get_now() -> str:
    """Returns today's date with hour & minute appended"""
    return datetime.datetime.now().strftime("%Y%m%d_%H%M")


def sanitize_str_for_bq(s: str) -> str:
    return s.replace("-", "_")


class MSSQLtoBigQuery:
    """MSSQLtoBigQuery orchestrator class"""

    def __init__(
        self,
        ingestion_type: str,
        database: str,
        schema: str,
        mssql_object_name: str,
        bq_table_name: str = None,
        chunks: int = 250000,
        keys: Union[str, list] = None,
        sql_query: str = None,
        last_val_column: str = None,
        last_val_query: str = None,
        last_val_default: Union[str, float, int] = None,
        incremental_keys: Union[str, list] = None,
    ):
        """_summary_

        Parameters
        ----------
            ingestion_type : str
                `full_replace` : data from source fully replaces any existing destination
                `append` : data from source is limited, based on values > latest value in GCP
                `upsert` : append, but new values are first inserted in a staging table,
                    matching identifiers are removed from the destination, then all new values are inserted.
            database : str
                MSSQL database name
            schema : str
                MSSQL schema name
            mssql_object_name : str
                MSSQL object name
            bq_table_name (str, optional): str, default None
                BigQuery table name (only if different than object name)
            chunks (int, optional): int, default 250000
                Chunk size for querying MSSQL db
            index (Union[str, list], optional): Union[str, list], default None
                Str of single primary key OR list of multiple primary keys
            sql_query (str, optional): str, default None
                Path to custom SQL query for extract, relative to `sql/mssql`
            last_val_column (str, optional): str, default None
                Column to reference for append/upsert ingestion (i.e. `last_updated_datetime`)
            last_val_query (str, optional): str, default None
                Path to custom SQL query for getting the last_val from BQ, relative to `sql/bigquery`
            last_val_default (Union[str, float, int], optional): Union[str, float, int], default None
                Default value to use as minimum if destination does not exist - leave as None if not needed
            incremental_keys (Union[str, list], optional): Union[str, list], default None
                Str or list of str (keys to match staging <> destination) and purge
        """
        self.gcp_project = os.environ["gcp_project"]
        self.gcp_dataset = os.environ["gcp_dataset"]
        self.bucket = os.environ["gcs_bucket"]
        self.gcs_prefix = os.environ["gcs_prefix"]
        self.mssql_hostname = os.environ["mssql_hostname"]
        self.mssql_username = os.environ.get("mssql_username")
        self.mssql_password = os.environ.get("mssql_password")
        self.mssql_port = os.environ.get("mssql_port")

        self.ingestion_type = ingestion_type
        self.incremental = INGESTION_TYPES.get(self.ingestion_type).get("incremental")
        self.check_last_val = INGESTION_TYPES.get(self.ingestion_type).get("last_val")
        self.source_format = INGESTION_TYPES.get(self.ingestion_type).get(
            "source_format"
        )
        self.write_disposition = INGESTION_TYPES.get(self.ingestion_type).get(
            "write_type"
        )

        self.database = database
        self.schema = schema
        self.mssql_object_name = mssql_object_name
        self.bq_table_name = sanitize_str_for_bq(
            next(tbl for tbl in [bq_table_name, mssql_object_name] if tbl is not None)
        )
        self.destination_project_dataset_table = (
            f"{self.gcp_project}.{self.gcp_dataset}.{self.bq_table_name}"
        )
        self.landing_project_dataset_table = (
            f"{self.gcp_project}.{self.gcp_dataset}.stg_{self.bq_table_name}"
            if self.incremental
            else self.destination_project_dataset_table
        )
        self.dt_suffix = get_now()
        self.gcs_filename_format = f"{self.gcs_prefix}/{self.database}/{self.schema}/{self.table}/{self.table}_{self.dt_suffix}_{{}}.json"
        self.gcs_uri_reference = [
            f"{self.gcs_prefix}/{self.database}/{self.schema}/{self.table}/{self.table}_{self.dt_suffix}_*.json"
        ]

        self.chunks = chunks
        self.keys = keys if type(keys) == list else [keys]
        self.sql_query = sql_query
        self.last_val_column = last_val_column
        self.last_val_query = last_val_query
        self.last_val_default = last_val_default
        self.incremental_keys = (
            incremental_keys if type(incremental_keys) == list else [incremental_keys]
        )

        self.mssql_conn = self._get_mssql_conn()
        self.bq_client = bigquery.Client(project=self.gcp_project)
        self.gcs_client = storage.Client(project=self.gcp_project)
        self.gcs_bucket = self.gcs_client.bucket(self.bucket)

        self.last_val = None
        self.query_str = None
        self.schema_json = None
        self.file_list = []
        self.file_num = 0

    def _get_mssql_conn(self):
        """Establishes & returns pyodbc Connection to MSSQL
        Assumes trusted_connection unless both username & password
            are set via env variables
        ## -> pyodbc.Connection:
        """
        sql_conn = pyodbc.connect(
            f"Driver={{{MSSQL_DRIVER}}};"
            f"Server={self.mssql_hostname};"
            f"Database={self.database};"
            + (
                f"Trusted_Connection=yes;"
                if any(
                    elem is None for elem in [self.mssql_username, self.mssql_password]
                )
                else f"UID={self.mssql_username};PWD={self.mssql_password};"
            )
        )
        return sql_conn

    def _get_last_val(self):
        """"""
        if self.last_val_query:
            query_str = read_file(
                f"sql/bigquery/{self.last_val_query}",
                gcp_project=self.gcp_project,
                gcp_dataset=self.gcp_dataset,
            )
        else:
            query_job = self.bq_client.query(
                read_file(
                    f"sql/bigquery/check_data_type.sql",
                    gcp_project=self.gcp_project,
                    gcp_dataset=self.gcp_dataset,
                    table_name=self.bq_table_name,
                    field_name=self.check_column,
                )
            )
            result = query_job.result()
            data_type = next(result).data_type
            if data_type in ["DATE", "DATETIME", "TIME", "TIMESTAMP"]:
                query_str = (
                    f"SELECT\n"
                    f"""MAX(FORMAT_TIMESTAMP("%F %H:%M:%E3S",{self.check_column})) AS `last_val`"""
                    f"FROM `{self.gcp_project}.{self.gcp_dataset}.{self.bq_table_name}`;"
                )
            else:
                query_str = (
                    f"SELECT MAX({self.check_column}) AS `last_val`"
                    f"FROM `{self.gcp_project}.{self.gcp_dataset}.{self.bq_table_name}`;"
                )
        try:
            logging.info(f"Running last val query: {query_str}")
            query_job = self.bq_client.query(query_str)
            result = query_job.result()
            last_val = next(result.last_val)
            logging.info(f"Last Val: {last_val}")
        except NotFound as error:
            logging.warning(f"Error getting last value: {error}")
            if self.default_last_val:
                last_val = self.default_last_val
                logging.info(f"Using default last val: {last_val}")

        return last_val

    def _get_query_str(self) -> str:
        """Get (or build) SQL Query string
        If custom query path passed, read it in & pass ANY args used
        Otherwise, generate a query string based on the args/attributes of the class
        """
        if self.query:
            query_str = read_file(
                f"sql/mssql/{self.dataset_name}/{self.query}.sql",
                database=self.database,
                schema=self.schema,
                table=self.mssql_object_name,
                check_column=self.check_column,
                last_val=self.last_val,
            )
        else:
            schema_df = self._get_mssql_schema()
            query_str = (
                "SELECT \n"
                + "\n,".join(
                    [
                        (
                            (
                                f"CAST([{r['col_name']}] AS BIGINT) AS "
                                if r["col_type"] == "bigint"
                                else ""
                            )
                            + f"[{r['col_name']}]"
                        )
                        for _, r in schema_df.iterrows()
                    ]
                )
                + f"\nFROM {self.database}.{self.schema}.{self.mssql_object_name} (NOLOCK)"
                + (
                    f"\nWHERE {self.check_column} > '{self.last_val}'"
                    if self.check_column
                    else ""
                )
            )
        logging.info(f"Got sql query:\n{query_str}\n")
        return query_str

    def _get_mssql_schema(self) -> pd.DataFrame:
        schema_query = read_file(
            "sql/mssql/schema.sql",
            database=self.database,
            schema=self.schema,
            table=self.mssql_object_name,
        )
        schema_df = pd.read_sql(schema_query, self.mssql_conn)

        if self.query:
            sample_df = pd.read_sql(
                self.query_str.replace("SELECT", "SELECT TOP 1"), self.mssql_conn
            )
            fields = [f for f in sample_df.columns.values.tolist()]
            schema_df["is_field"] = schema_df.apply(
                lambda x: True if x.col_name in fields else False, axis=1
            )
        else:
            schema_df["is_field"] = True

        return schema_df

    def _get_schema(self) -> list:
        """Generate schema dataframe from SQL
        If a custom query exists (likely to limit columns), limit schema to selected cols
        Add bool for whether a field is included in the custom query SELECT
        Iterate over schema records, generate a schema JSON to be used later (BQ Schema Fields)
            Check for data dictionary descriptions and add
        """
        schema_df = self._get_mssql_schema()

        schema_json = [
            {
                "name": row.bq_name,
                "field_type": TYPE_MAPPING.get(row.col_type, "STRING"),
                "mode": "NULLABLE" if row.is_nullable else "REQUIRED",
            }
            for row in schema_df.itertuples()
            if row.is_field
        ]
        logging.info(
            f"Got schema for {self.database}.{self.schema}.{self.mssql_object_name}"
        )
        logging.info(f"{schema_json}")
        return schema_json

    def _get_data(self):
        """Extract data from MSSQL connection
        Perform iterations based on the chunks parameter
        For each iteration
            Store a list of dict indicating GCS & local file paths
            Write json dump'd list of dict, zipped with col name & converted value
        """
        logging.info(
            f"Getting data for {self.database}.{self.schema}.{self.mssql_object_name}"
        )
        mssql_cursor = self.mssql_conn.cursor()
        mssql_cursor.execute(self.query_str)

        while True:
            rows = mssql_cursor.fetchmany(self.chunks)
            if not rows:
                break
            file_name = self.gcs_filename_format.format(self.file_num)
            local_file = f"data/{os.path.basename(file_name)}"
            self.file_list.append(
                {"gcs_path": file_name, "local_path": local_file, "uploaded": False}
            )
            with open(local_file, "wb") as fp:
                [
                    fp.write(
                        json.dumps(
                            dict(
                                zip(
                                    [s["name"] for s in self.schema_json],
                                    [convert_type(x) for x in row],
                                )
                            ),
                            ensure_ascii=False,
                        ).encode("utf-8")
                        + b"\n"
                    )
                    for row in rows
                ]
                logging.info(f"Stored {local_file}..")
                self.file_num += 1
            self._load_files_to_gcs()

    def _load_files_to_gcs(self):
        """Replacement for `_load_data_to_gcs`
        This supports larger ingestion by loading to GCS as-we-go
        gcs is blob name, local is local relative path
        """
        for file in [f for f in self.file_list if not f["uploaded"]]:
            blob = self.gcs_bucket.blob(file["gcs_path"])
            blob.upload_from_filename(file["local_path"])
            file["uploaded"] = True
            os.remove(file["local_path"])
            logging.info(f"Uploaded file to {file['gcs_path']}, deleted local..")

    def _load_gcs_to_bq(self):
        gcs_uris = [f"gs://{self.bucket}/{x}" for x in self.gcs_files]
        gcp_schema = [
            bigquery.SchemaField(
                name=f["name"],
                field_type=f["field_type"],
                mode=f["mode"],
                description=f.get("description", None),
            )
            for f in self.schema_json
        ]
        job_config = bigquery.LoadJobConfig(
            schema=gcp_schema,
            write_disposition=self.write_disposition,
            source_format=self.source_format,
        )
        try:
            load_job = self.bq_client.load_table_from_uri(
                gcs_uris, self.landing_project_dataset_table, job_config=job_config
            )
        except BadRequest as ex:
            for err in ex.errors:
                logging.critical(f"Logging error: {err}")

        if self.incremental:

            query = read_file(
                "sql/bigquery/incremental.sql",
                dataset=self.dataset,
                table=self.gcp_table,
                uuid=", ".join(self.uuid),
            )
            try:
                result = self.bq_client.query(query)
                result.result()
            except Exception as e:
                if "400 Not found" in str(e):
                    query = read_file(
                        "sql/bigquery/incremental_first.sql",
                        dataset=self.dataset,
                        table=self.gcp_table,
                    )
                    result = self.bq_client.query(query)
                    result.result()
                else:
                    logging.critical(f"Error with incremental load query: {e}")
                    raise Exception(e)

                logging.info(f"Ran incremental query:\n{query}")
        else:
            logging.info(f"Completed loading to BQ {self.gcp_load_table}..")

    def _check_keys(self):
        key_cols = ",".join(self.keys)
        key_check_query = (
            f"SELECT {key_cols}\n"
            f"FROM `{self.destination_project_dataset_table}`\n"
            f"GROUP BY {key_cols}\n"
            f"HAVING COUNT(*) > 1\n"
            f"ORDER BY COUNT(*) DESC\n"
            f"LIMIT 100;"
        )
        result = self.bq_client.query(key_check_query).to_dataframe()
        if len(result) > 0:
            logging.critical(f"Warning: duplicate records found:\n")
            logging.critical(result.iloc[0:5])
        else:
            logging.info(f"Result passed index check.")
        return

    def execute(self):
        logging.info(
            f"""
        ---------------------------------------------------------------------------
        Starting ingestion for {self.database}.{self.schema}.{self.bq_table_name}
        ---------------------------------------------------------------------------
        """
        )
        if self.check_last_val:
            self.last_val = self._get_last_val()
        self.query_str = self.get_query_str()
        self.schema_json = self._get_schema()
        self._get_data()
        if self.file_num == 0:
            logging.warning(f"No files found to load to GCP..")
            return
        self._load_gcs_to_bq()
        if self.incremental:
            self._load_staging_to_dest()
        if self.index:
            self._check_index()
