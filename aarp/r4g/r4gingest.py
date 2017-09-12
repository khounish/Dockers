import airflow
from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.python_operator import PythonOperator
import pysftp as sftp
import requests
import json, yaml


def filelanding():
    #airflow_env=loadEnvVariables()
    
    
    with open('r4gingest.yaml') as f:
        CONFIG = yaml.load(f)
    f.close()
    
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
    with open('r4gingest.yaml') as f:
        CONFIG = yaml.load(f)
    f.close()
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
    
    with open('r4gingest.yaml') as f:
        CONFIG = yaml.load(f)
    f.close()
    
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




