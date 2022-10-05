from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from datetime import datetime, date
import requests
import json
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Urls to download the data from swapi.dev
source_people_url = 'https://swapi.dev/api/people/'
source_films_url = 'https://swapi.dev/api/films/'


default_args = {
    'owner': 'Ram',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 3),
    'email': ['xxxx@xxxxx.com'],
    'email_on_failure': False,
    'postgres_conn_id': 'postgres_local'
}

postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
engine = postgres_hook.get_sqlalchemy_engine()

# getting api data and save it in to the table
# We are able to query max 10 records so need to query the data recursively
# loading data in to postgres sql db
# using postgres hook  copying data in bulk to the db instead of writing record by record
# Overwriting the data everytime with new data

def get_star_wars_data(input_file, table_name):
    response = requests.get(input_file)
    data = json.loads(response.text)
    count = data['count']
    i = 1
    rows_count = 0
    df = pd.DataFrame()
    while rows_count != count:
        response = requests.get(input_file + '?page=' + str(i))
        data = json.loads(response.text)
        df_temp = pd.DataFrame(data['results'])
        df = pd.concat([df, df_temp])
        rows_count = df.shape[0]
        i = i + 1
    df.to_sql(table_name,engine,if_exists='replace' )
    





# defining dag
dag = DAG(
    'ingest_star_wars',
    default_args=default_args,
    schedule_interval="@once",
    template_searchpath='/opt/airflow/scripts/sql/star_wars/',
    catchup=False,
)
# Task for ingest raw people api data using python operator

get_raw_star_wars_people_data = PythonOperator(
    task_id='get_raw_star_wars_people_data',
    python_callable=get_star_wars_data,
    op_kwargs={'table_name': 'star_wars_people', 'input_file': source_people_url},
    dag=dag
)

# Task for Creating table for people api data
Create_star_wars_people_table = PostgresOperator(
    task_id='create_people_table',
    sql='create_people_table.sql',
    params={"table_name_sql": "star_wars_people"},
    dag=dag
)



# Task for ingest raw films api data using python operator

get_raw_star_wars_films_data = PythonOperator(
    task_id='get_raw_star_wars_films_data',
    python_callable=get_star_wars_data,
    op_kwargs={'table_name': 'star_wars_films', 'input_file': source_films_url},
    dag=dag
)

# Task for Creating table for films api data
Create_star_wars_films_table = PostgresOperator(
    task_id='create_films_table',
    sql='create_films_table.sql',
    params={"table_name_sql": "star_wars_films"},
    dag=dag
)


# Created a dummy to identify both apis ingestion successfully started

ingestion_Started = DummyOperator(
    task_id='ingestion_Started',
    trigger_rule="none_failed",
    dag=dag
)

# triggering the next dag upon successfull of this DAG

trigger_star_wars_agg_dag = TriggerDagRunOperator(
        task_id="trigger_star_wars_agg_dag",
        trigger_dag_id="agg_star_wars",
        wait_for_completion=False,
        dag=dag
)


# Ordering of tasks to be executed

ingestion_Started>>Create_star_wars_films_table >> get_raw_star_wars_films_data

ingestion_Started>>Create_star_wars_people_table >> get_raw_star_wars_people_data

[get_raw_star_wars_films_data,get_raw_star_wars_people_data] >> trigger_star_wars_agg_dag
