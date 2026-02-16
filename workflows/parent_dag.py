"""
============================================================================================
File: parent_dag.py
Project: healthcare-project-487014
Layer: Orchestration (Master Scheduler)

Description:
    Parent orchestration DAG responsible for coordinating the execution
    of the full data pipeline.

    Execution Flow:
        1. Trigger PySpark ingestion/transformation DAG
        2. After successful completion, trigger BigQuery transformation DAG

    This DAG is externally triggered (Cloud Build → Composer) and does not
    run on a schedule.
============================================================================================
"""

import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator


# ============================================================================================
# DEFAULT DAG ARGUMENTS
# ============================================================================================

DEFAULT_ARGS = {
    "owner": "EDUARDO BANDEIRA",
    "start_date": days_ago(1),
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
    dag_id="parent_dag",
    schedule_interval="0 5 * * *",  
    description="Parent DAG that orchestrates PySpark and BigQuery pipelines.",
    default_args=DEFAULT_ARGS,
    tags=["orchestration", "parent", "etl"],
) as dag:

    # ----------------------------------------------------------------------------------------
    # Trigger PySpark DAG
    # ----------------------------------------------------------------------------------------
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dag",
        wait_for_completion=True,
    )

    # ----------------------------------------------------------------------------------------
    # Trigger BigQuery DAG
    # ----------------------------------------------------------------------------------------
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_dag",
        wait_for_completion=True,
    )


# ============================================================================================
# TASK DEPENDENCIES
# ============================================================================================

trigger_pyspark_dag >> trigger_bigquery_dag
