from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from adobe.landing import extractTar
from datetime import datetime

dag = DAG(dag_id='adobe_landing_dag',
    start_date=datetime(2017,8,15),
    catchup= False,
    schedule_interval='@hourly')

t2 = PythonOperator(
    task_id='adobe_untar',
    python_callable=extractTar,
    dag=dag
)