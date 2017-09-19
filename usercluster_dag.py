from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from aarp.common.utils import createCluster

from datetime import datetime, timedelta

def createUserCluster():
    createCluster(name="userCluster")


# Following are defaults which can be overridden later on
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 15),
    'email': ['swapnilspra@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


startDag = DAG(
    dag_id='start_cluster',default_args=default_args,
    start_date=datetime(2017,7,15),
    #catchup= False,
    schedule_interval='0 11 * * 1-5')

t1 = PythonOperator(
    task_id='start_cluster',
    python_callable=createUserCluster,
    dag=startDag
)
