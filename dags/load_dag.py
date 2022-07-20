import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


def read_data():
    df = pd.read_csv('/opt/airflow/data/20181024_d1_0830_0900.csv', sep='[,;:]', index_col=False)
    return df.shape


def insert_data():
    pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = pg_hook.get_sqlalchemy_engine()
    df = pd.read_csv("/opt/airflow/data/20181024_d1_0830_0900.csv", sep='[,;:]', index_col=False)

    df.to_sql(
        "open_traffic",
        con=conn,
        if_exists="replace",
        index=False,
    )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['diyye101@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    "start_date": datetime(2022, 7, 20, 2, 30, 00),
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        "load_dag",
        default_args=default_args,
        schedule_interval="*/1 * * * *",
        catchup=False,
) as dag:
    read_data_op = PythonOperator(
        task_id="read_data", python_callable=read_data
    )
    create_table_op = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            create table if not exists open_traffic (
                   track_id INT NOT NULL,
                   type TEXT DEFAULT NULL,
                   traveled_d FLOAT DEFAULT NULL,
                   avg_speed FLOAT DEFAULT NULL, 
                   lat FLOAT DEFAULT NULL,
                   lon FLOAT DEFAULT NULL,
                   speed FLOAT DEFAULT NULL,
                   lon_acc FLOAT DEFAULT NULL,
                   lat_acc FLOAT DEFAULT NULL,
                   time FLOAT NULL DEFAULT NULL,
                   primary key (track_id)
               )
           """,
    )
    load_data_op = PythonOperator(
        task_id="load_data", python_callable=insert_data
    )

read_data_op >> create_table_op >> load_data_op
