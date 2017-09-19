from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from aarp.mnthlyscores.mnthlyscores import mnthlyscores
from datetime import datetime, timedelta

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



# adding customised parameters
dag = DAG(dag_id='main_monthly_dag',default_args=default_args,start_date=datetime(2017,9,15),schedule_interval='@mnthly')

mnthlyscores = PythonOperator(
    task_id='mnthlyscores',
    python_callable=mnthlyscores,
    dag=dag
)
