from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from ETL.scripts.extract import extract_table
from ETL.scripts.transform import transform_absence
import requests
import json
import os


# Define the default dag arguments.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hamza.landolsi@soprahr.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


# Define the main DAG
with DAG(
    dag_id='ETL_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(3),
    catchup=False,
) as dag:
    extract_data = PythonOperator(
        task_id='extract_data',
        provide_context=True,
        python_callable=extract_table,
        op_args=['dimabsence'],
    )
"""
    transform_data = PythonOperator(
        task_id='transform_data',
        provide_context=True,
        python_callable=transform_absence,
        op_args=['{{ task_instance.xcom_pull(task_ids="extract_data") }}'],  # Use XComArg
    )

    # Set up task dependencies
    extract_data >> transform_data

"""
#port = 5432





