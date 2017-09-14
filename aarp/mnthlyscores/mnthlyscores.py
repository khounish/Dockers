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


def mnthlyscores:     
    postdata = {
      "run_name": "mnthlyscores",
      "existing_cluster_id":clusterid['cluster_id'],
      "notebook_task": {
        "notebook_path": CONFIG['imax']['mnthlyscores']['notebook_path'],
        "base_parameters":{"pathDataLakeImax":CONFIG['imax']['mnthlyscores']['pathDataLakefcom'],"pathLandingImax":CONFIG['imax']['mnthlyscores']["pathLandingfcom"]}
      }
    }
    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/submit"
    res = requests.post(url, auth=(CONFIG['user'],CONFIG['pwd']), json=postdata)
    runid = json.loads(res.text)["run_id"]
    print(runid)
    monitorJob(runid)
