from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from aarp.mnthlyscores.mnthlyscores import mnthlyscores


# adding customised parameters
dag = DAG(
    dag_id='main_dag',
    start_date=datetime(2017,9,15),
    catchup= False,
    schedule_interval='@mnthly')

mnthlyscores = PythonOperator(
    task_id='mnthlyscores',
    python_callable=mnthlyscores,
    dag=dag
)
