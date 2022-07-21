from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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
    "dbt_dag",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
) as dag:
    dbt_debug = BashOperator(
        task_id="dbt_debug", bash_command="cd ../dbt && dbt debug"
    )
    dbt_run = BashOperator(
        task_id="dbt_run", bash_command="cd ../dbt && dbt run"
    )
    dbt_test = BashOperator(
        task_id="dbt_test", bash_command="cd ../dbt && dbt test"
    )

dbt_debug >> dbt_run >> dbt_test
