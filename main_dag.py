from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from utils import createCluster
from datetime import datetime

dag = DAG(
    dag_id='main_dag',
    start_date=datetime(2017,6,15),
    catchup= False,
    schedule_interval='@daily')

t1 = BashOperator(
    task_id='doubleclick_ingest',
    bash_command='python /data/airflow/pythonscripts/doubleclick_file_transfer.py',
    dag=dag)

t2 = PythonOperator(
    task_id='doubleclick_impressions',
    python_callable=createCluster,
    dag=dag
)

t3 = PythonOperator(
    task_id='doubleclick_click',
    python_callable=createCluster,
    dag=dag
)

t4 = PythonOperator(
    task_id='doubleclick_activity',
    python_callable=createCluster,
    dag=dag
)

t5 = PythonOperator(
    task_id='doubleclick_archive',
    python_callable=createCluster,
    dag=dag
)

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t1)
t5.set_upstream(t2)
t5.set_upstream(t3)
t5.set_upstream(t4)