from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from aarp.adobe.landing import extractTar
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 15),
    'email': ['swapnilspra@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(dag_id='adobe_landing_dag',start_date=datetime(2017,9,19),default_args=default_args,schedule_interval='@hourly')


t1 = PythonOperator(
    task_id='adobe_untar',
    python_callable=extractTar,
    dag=dag
)

