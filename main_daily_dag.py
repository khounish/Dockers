from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from aarp.common.utils import createCluster
from aarp.adobe.landing import extractTar
from aarp.adobe.lake import startAdobeLakeJob, startUTCJob
from aarp.r4g.r4gingest import filelanding,checkclusterstatus,jobrun
from aarp.fcom.fcom import fcomlanding,fcomrsload



# adding customised parameters
dag = DAG(
    dag_id='main_dag',
    start_date=datetime(2017,9,15),
    catchup= False,
    schedule_interval='@daily')

adobe1 = PythonOperator(
    task_id='adobe_untar',
    python_callable=extractTar,
    dag=dag
)

adobe2 = PythonOperator(
    task_id='adobe_lake',
    python_callable=startAdobeLakeJob,
    dag=dag
)

adobe3 = PythonOperator(
    task_id='adobe_UTC',
    python_callable=startUTCJob,
    dag=dag
)

r4g_ingest = PythonOperator(
    task_id='r4g_ingest',
    python_callable=filelanding,
    dag=dag
)

r4g_local = PythonOperator(
    task_id='r4g_local',
    python_callable=jobrun('local'),
    dag = dag
)

r4g_auction = PythonOperator(
    task_id='r4g_auction',
    python_callable=jobrun('auction'),
    dag = dag
)

r4g_merch = PythonOperator(
    task_id='r4g_merch',
    python_callable=jobrun('merch'),
    dag = dag
)

r4g_point = PythonOperator(
    task_id='r4g_point',
    python_callable=jobrun('point'),
    dag = dag
)

r4g_promo = PythonOperator(
    task_id='r4g_promo',
    python_callable=jobrun('promo'),
    dag = dag
)

r4g_reg = PythonOperator(
    task_id='r4g_reg',
    python_callable=jobrun('reg'),
    dag = dag
)

r4g_sweep = PythonOperator(
    task_id='r4g_sweep',
    python_callable=jobrun('sweep'),
    dag = dag
)

r4g_travel = PythonOperator(
    task_id='r4g_travel',
    python_callable=jobrun('travel'),
    dag = dag
)

r4g_rs_load = PythonOperator(
    task_id='r4g_rs_load',
    python_callable=jobrun('travel'),
    dag = dag
)

imax_fcom_landing = PythonOperator(
    task_id='imax_fcom_landing',
    python_callable=fcomlanding,
    dag = dag
)

imax_fcom_rsload = PythonOperator(
    task_id='imax_fcom_rsload',
    python_callable=fcomrsload,
    dag = dag
)

adobe2.set_upstream(adobe1)
adobe3.set_upstream(adobe2)

r4g_ingest.set_upstream(adobe3)
r4g_local.set_upstream(r4g_ingest)
r4g_merch.set_upstream(r4g_ingest)
r4g_point.set_upstream(r4g_ingest)
r4g_promo.set_upstream(r4g_ingest)
r4g_reg.set_upstream(r4g_ingest)
r4g_sweep.set_upstream(r4g_ingest)
r4g_travel.set_upstream(r4g_ingest)

r4g_rs_load.set_upstream(r4g_local)
r4g_rs_load.set_upstream(r4g_merch)
r4g_rs_load.set_upstream(r4g_point)
r4g_rs_load.set_upstream(r4g_promo)
r4g_rs_load.set_upstream(r4g_reg)
r4g_rs_load.set_upstream(r4g_sweep)
r4g_rs_load.set_upstream(r4g_travel)

imax_fcom_landing.set_upstream(r4g_rs_load)
imax_fcom_rsload.set_upstream(imax_fcom_landing)

