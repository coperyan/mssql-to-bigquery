import os
import yaml
from dataclasses import dataclass

with open("config.yml", "r") as f:
    CONFIG = yaml.safe_load(f)


def get_env_cfg() -> dict:
    return {
        "gcp_project": CONFIG.get("gcp_project", os.environ["gcp_project"]),
        "gcp_dataset": CONFIG.get("gcp_dataset", os.environ["gcp_dataset"]),
        "gcs_bucket": CONFIG.get("gcs_bucket", os.environ["gcs_bucket"]),
        "gcs_prefix": CONFIG.get(
            "gcs_prefix", os.environ.get("gcs_prefix", "mssql-ingestion")
        ),
        "mssql_hostname": CONFIG.get("mssql_hostname", os.environ["mssql_hostname"]),
        "mssql_username": CONFIG.get(
            "mssql_username", os.environ.get("mssql_username")
        ),
        "mssql_password": CONFIG.get(
            "mssql_password", os.environ.get("mssql_password")
        ),
    }
