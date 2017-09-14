import airflow
from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.python_operator import PythonOperator
import requests
import json, yaml
from aarp.common.utils import loadYAMLEnvVariables,checkForProdCluster,clusteridcreate

CONFIG=loadEnvVariables()
clusterid=checkForProdCluster(CONFIG['cluster_name'])

if clusterid['cluster_id'] is null:
	clusterid=clusteridcreate(CONFIG['cluster_name'])


def fcomlanding:     
    postdata = {
      "run_name": "fcom_landing",
      "existing_cluster_id":clusterid['cluster_id'],
      "notebook_task": {
        "notebook_path": CONFIG['landing_notebook_path'],
        "base_parameters":{"pathDataLakeImax":CONFIG['imax']['fcom']['pathDataLakefcom'],"pathLandingImax":CONFIG['imax']['fcom']["pathLandingfcom"]}
      }
    }
    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/submit"
    res = requests.post(url, auth=(CONFIG['user'],CONFIG['pwd']), json=postdata)
    runid = json.loads(res.text)["run_id"]
    print(runid)
    monitorJob(runid)
	
def fcomrsload:
    print(paramjson["clusterid"])
      
    postdata = {
      "run_name": "fcomrsload",
      "existing_cluster_id":clusterid['cluster_id'],
      "notebook_task": {
        "notebook_path": CONFIG['imax']['fcom']['rs_notebook_path'],
        "base_parameters":{"pathDataLakeImax":CONFIG['imax']['fcom']['pathDataLakefcom'],"pathLandingImax":CONFIG['imax']['fcom']["pathLandingfcom"]}
      }
    }
    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/submit"
    res = requests.post(url, auth=(CONFIG['imax']['fcom']['user'],CONFIG['imax']['fcom']['pwd']), json=postdata)
    runid = json.loads(res.text)["run_id"]
    print(runid)
    monitorJob(runid)
