"""
============================================================================================
File: bq_dag.py
Project: healthcare-project-487014
Layer: Orchestration (BigQuery Execution)

Description:
    Airflow DAG responsible for executing the Medallion Architecture SQL layers
    in BigQuery (Bronze → Silver → Gold).

    The DAG:
        1. Reads SQL scripts stored in the Composer GCS-mounted directory
        2. Executes them sequentially using BigQueryInsertJobOperator
        3. Runs in batch mode (cost-efficient execution)

    Execution order:
        Bronze  →  Silver  →  Gold

    This DAG is externally triggered (Cloud Build → Composer) and does not
    run on a schedule.
============================================================================================
"""

import airflow
from airflow import DAG
from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# ============================================================================================
# CONSTANTS
# ============================================================================================

PROJECT_ID = "healthcare-project-487014"
LOCATION = "US"

SQL_FILE_PATH_BRONZE = "/home/airflow/gcs/data/BQ/bronze.sql"
SQL_FILE_PATH_SILVER = "/home/airflow/gcs/data/BQ/silver.sql"
SQL_FILE_PATH_GOLD = "/home/airflow/gcs/data/BQ/gold.sql"


# ============================================================================================
# HELPER FUNCTIONS
# ============================================================================================

def read_sql_file(file_path: str) -> str:
    """
    Read a SQL file from the Composer GCS-mounted path.

    Parameters
    ----------
    file_path : str
        Absolute path of the SQL file.

    Returns
    -------
    str
        SQL query content.
    """
    with open(file_path, "r") as file:
        return file.read()


BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_BRONZE)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_SILVER)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_GOLD)


# ============================================================================================
# DEFAULT DAG ARGUMENTS
# ============================================================================================

DEFAULT_ARGS = {
    "owner": "EDUARDO BANDEIRA",
    "start_date": None,  
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================================================
# DAG DEFINITION
# ============================================================================================

with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    description="Executes Bronze, Silver, and Gold BigQuery transformations.",
    default_args=DEFAULT_ARGS,
    tags=["gcs", "bq", "etl", "medallion"],
) as dag:

    # ----------------------------------------------------------------------------------------
    # Bronze Layer
    # ----------------------------------------------------------------------------------------
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

    # ----------------------------------------------------------------------------------------
    # Silver Layer
    # ----------------------------------------------------------------------------------------
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

    # ----------------------------------------------------------------------------------------
    # Gold Layer
    # ----------------------------------------------------------------------------------------
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )


# ============================================================================================
# TASK DEPENDENCIES
# ============================================================================================

bronze_tables >> silver_tables >> gold_tables
