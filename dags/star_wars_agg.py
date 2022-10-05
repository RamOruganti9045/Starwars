from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from datetime import datetime, date
import requests
import json
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor



default_args = {
    'owner': 'Ram',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 3),
    'email': ['xxxx@xxxxx.com'],
    'email_on_failure': False,
    'postgres_conn_id': 'postgres_local'
}





dag = DAG(
    'agg_star_wars',
    default_args=default_args,
    schedule_interval='@once',
    template_searchpath='/opt/airflow/scripts/sql/star_wars/',
    catchup=False,
)




# Task to create table for aggregated data
Create_star_wars_agg_table = PostgresOperator(
    task_id='create_agg_table',
    sql='create_agg_table.sql',
    params={"table_name_sql": "star_wars_agg_data"},
    dag=dag
)

# task for to transform and create aggregated data
aggregation_of_data = PostgresOperator(
    task_id='aggregation_of_data',
    sql='star_wars_agg.sql',
    params={"people_table_name": "star_wars_people",
            "films_table_name": "star_wars_films",
            "agg_table_name": "star_wars_agg_data",
            },
    dag=dag
)

# ordering of tasks to be executed

Create_star_wars_agg_table >> aggregation_of_data
