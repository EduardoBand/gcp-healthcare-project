"""
============================================================================================
File: pyspark_dag.py
Project: healthcare-project-487014
Layer: Ingestion (Dataproc / PySpark)

Description:
    This DAG is responsible for orchestrating the ingestion layer using
    Google Cloud Dataproc. It performs the following steps:

        1. Starts a Dataproc cluster
        2. Executes PySpark ingestion jobs for:
            - Hospital A (MySQL → Landing)
            - Hospital B (MySQL → Landing)
            - Claims ingestion
            - CPT Codes ingestion
        3. Stops the Dataproc cluster

    This DAG is externally triggered (Cloud Build → Composer) and does not
    run on a schedule.
============================================================================================
"""

# ============================================================================================
# IMPORTS
# ============================================================================================

import airflow
from airflow import DAG
from datetime import timedelta
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)


# ============================================================================================
# PROJECT CONFIGURATION
# ============================================================================================

PROJECT_ID = "healthcare-project-487014"
REGION = "us-east1"
CLUSTER_NAME = "my-demo-cluster2"

# Composer bucket where ingestion scripts are stored
COMPOSER_BUCKET = "us-east1-demo-instance-a60ecfc7-bucket"

# ============================================================================================
# PYSPARK JOB DEFINITIONS
# ============================================================================================

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_1},
}

GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"
PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_2},
}

GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_3},
}

GCS_JOB_FILE_4 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"
PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_4},
}


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
    dag_id="pyspark_dag",
    schedule_interval=None,  # Triggered externally (Cloud Build → Composer)
    description="Dataproc ingestion pipeline for healthcare-project-487014",
    default_args=DEFAULT_ARGS,
    tags=["pyspark", "dataproc", "etl"],
) as dag:

    # ----------------------------------------------------------------------------------------
    # Start Dataproc Cluster
    # ----------------------------------------------------------------------------------------
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # ----------------------------------------------------------------------------------------
    # PySpark Ingestion Tasks
    # ----------------------------------------------------------------------------------------

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_hospital_a",
        job=PYSPARK_JOB_1,
        region=REGION,
        project_id=PROJECT_ID,
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_hospital_b",
        job=PYSPARK_JOB_2,
        region=REGION,
        project_id=PROJECT_ID,
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id="pyspark_claims",
        job=PYSPARK_JOB_3,
        region=REGION,
        project_id=PROJECT_ID,
    )

    pyspark_task_4 = DataprocSubmitJobOperator(
        task_id="pyspark_cpt_codes",
        job=PYSPARK_JOB_4,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ----------------------------------------------------------------------------------------
    # Stop Dataproc Cluster
    # ----------------------------------------------------------------------------------------
    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )


# ============================================================================================
# TASK DEPENDENCIES
# ============================================================================================

start_cluster >> pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3 >> pyspark_task_4 >> stop_cluster
