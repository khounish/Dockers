import airflow
from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.python_operator import PythonOperator
import pysftp as sftp
import requests
import json, yaml


#Read yaml file
CONFIG = {}

with open('r4gingest.yaml') as f:
    CONFIG = yaml.load(f)
f.close()

print(CONFIG['localpath'])


def filelanding():
    s = sftp.Connection(host=CONFIG['host'],username=CONFIG['ingestuser'],password=CONFIG['ingestpwd'])

    localpath = CONFIG['localpath']

    remote_dir = CONFIG['remotedir']

    filelist = s.listdir(remote_dir)

    yesterday = (datetime.now() - timedelta(days = 1)).strftime("%Y%m%d")
    print(yesterday)

    print("Download Initiated...")

    for filename in filelist:
        filedate = filename[:8]
        if(filedate == yesterday):
            s.get(remote_dir + "/" + filename,localpath + "/" + filename)

    print("Files downloaded Successfully!")
    s.close()


def checkclusterstatus(clusterid):
    clusterstatus = ""
    postdata = {
        "cluster_id": clusterid
        }
    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/clusters/get"

    while clusterstatus != "RUNNING" and clusterstatus != "TERMINATED":
        res = requests.get(url, auth=(CONFIG['user'],CONFIG['pwd']),params=postdata)

        json_data = json.loads(res.text)
        clusterstatus = json_data["state"]
        print(clusterstatus)

    if clusterstatus == "TERMINATED":
        urlstart = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/clusters/start"
        res = requests.post(urlstart, auth=(CONFIG['user'],CONFIG['pwd']),json = {"cluster_id":clusterid})
        while clusterstatus != "RUNNING":
            res = requests.get(url, auth=(CONFIG['user'],CONFIG['pwd']),params=postdata)
        json_data = json.loads(res.text)
            clusterstatus = json_data["state"]
            print(clusterstatus)

        return clusterid
    else:
        return clusterid
    


def jobrun(paramjson):
    print(paramjson["clusterid"])
    postdata = {
      "run_name": "airflow_test_job",
      "existing_cluster_id":paramjson["clusterid"],
      "timeout_seconds": CONFIG['timeoutsecs'],
      "notebook_task": {
        "notebook_path": CONFIG['notebookpath'],
        "base_parameters":{"file_type":paramjson["filetype"],"schema_file_name":paramjson["schemaname"]}
      }
    }

    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/submit"
    res = requests.post(url, auth=(CONFIG['user'],CONFIG['pwd']), json=postdata)
    
    json_data = json.loads(res.text)
    runid = json_data["run_id"]

    print(runid)

    postdata = {
            "run_id": runid
            }

    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/get"
    res = requests.get(url, auth=(CONFIG['user'],CONFIG['pwd']),params=postdata)
    print(res.content)


def delcluster(clustid):
    print(clustid)

    postdata = {
            "cluster_id": clustid
            }
    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/clusters/delete"

    res = requests.post(url, auth=(CONFIG['user'],CONFIG['pwd']), json=postdata)
    print(res.content)


dag = DAG(
dag_id='r4gupload',
start_date=datetime(2017, 7, 14),
catchup= False,
schedule_interval='@daily')


t1 = PythonOperator(
    task_id='dwld_files_task',
    python_callable=filelanding(),
    dag=dag
)


t2 = PythonOperator(
    task_id='startcluster_task',
    python_callable=checkclusterstatus("0706-084812-ulnas345"),
    dag=dag
)

t2.set_upstream(t1)

jobrunjson = {
    "clusterid":t2.python_callable,
    "filetype" : CONFIG['localfile'],
    "schemaname":CONFIG['localschema']
    }

t3 = PythonOperator(
    task_id='jobrun_task3',
    python_callable=jobrun(jobrunjson),
    dag = dag
)
t3.set_upstream(t2)


jobrunjson = {
    "clusterid":t2.python_callable,
    "filetype" :CONFIG['auctionfile'],
    "schemaname":CONFIG['auctionschema']
    }

t4 = PythonOperator(
    task_id='jobrun_task4',
    python_callable=jobrun(jobrunjson),
    dag = dag
)
t4.set_upstream(t2)

jobrunjson = {
    "clusterid":t2.python_callable,
    "filetype" : CONFIG['merchfile'],
    "schemaname":CONFIG['merchschema']
    }

t5 = PythonOperator(
    task_id='jobrun_task5',
    python_callable=jobrun(jobrunjson),
    dag = dag
)
t5.set_upstream(t2)


jobrunjson = {
    "clusterid":t2.python_callable,
    "filetype" : CONFIG['pointfile'],
    "schemaname":CONFIG['pointschema']
    }

t6 = PythonOperator(
    task_id='jobrun_task6',
    python_callable=jobrun(jobrunjson),
    dag = dag
)
t6.set_upstream(t2)

jobrunjson = {
    "clusterid":t2.python_callable,
    "filetype" : CONFIG['promofile'],
    "schemaname":CONFIG['promoschema']
    }

t7 = PythonOperator(
    task_id='jobrun_task7',
    python_callable=jobrun(jobrunjson),
    dag = dag
)
t7.set_upstream(t2)

jobrunjson = {
    "clusterid":t2.python_callable,
    "filetype" : CONFIG['regfile'],
    "schemaname":CONFIG['regschema']
    }

t8 = PythonOperator(
    task_id='jobrun_task8',
    python_callable=jobrun(jobrunjson),
    dag = dag
)
t8.set_upstream(t2)

jobrunjson = {
    "clusterid":t2.python_callable,
    "filetype" : CONFIG['sweepile'],
    "schemaname":CONFIG['sweepschema']
    }

t9 = PythonOperator(
    task_id='jobrun_task9',
    python_callable=jobrun(jobrunjson),
    dag = dag
)
t9.set_upstream(t2)

jobrunjson = {
    "clusterid":t2.python_callable,
    "filetype" : CONFIG['travelfile'],
    "schemaname":CONFIG['travelschema']
    }

t10 = PythonOperator(
    task_id='jobrun_task10',
    python_callable=jobrun(jobrunjson),
    dag = dag
)
t10.set_upstream(t2)


t11 = PythonOperator(
    task_id='delculster_task',
    python_callable=delcluster(t2.python_callable),
    dag = dag
)


t11.set_upstream(t3)
t11.set_upstream(t4)
t11.set_upstream(t5)
t11.set_upstream(t6)
t11.set_upstream(t7)
t11.set_upstream(t8)
t11.set_upstream(t9)
t11.set_upstream(t10)


#t11.set_upstream(t6)
