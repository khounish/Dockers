from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from aarp.common.utils import destroyCluster


def shutdownUserCluster():
    destroyCluster(name='userCluster')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 9, 19),
    'email': ['swapnilspra@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

stopDag = DAG(dag_id='stop_cluster',default_args=default_args,start_date=datetime(2017,9,15),schedule_interval='0 22 * * 1-5')

t2 = PythonOperator(
    task_id='stop_cluster',
    python_callable=shutdownUserCluster,
    dag=stopDag
)
