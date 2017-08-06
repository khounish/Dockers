import requests
from time import sleep
from datetime import datetime, timedelta
from utils import createCluster, monitorJob

def runArchiving():
    clusterMetaData = createCluster()
    jobURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/get?job_id=9316"
    res = requests.get(url=jobURL, auth=('production@aarp.com', 'C@serta!23'))
    notebookDetails = res.json()
    notebookDetails['settings']['existing_cluster_id'] = clusterMetaData['cluster_id']
    if notebookDetails['settings']['name'] == 'doubleclick_archiving':
        resetUrl = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/reset"
        resetData = {
            "job_id": 9316,
            "new_settings": notebookDetails['settings']
        }
        requests.post(url=resetUrl, json=resetData, auth=('production@aarp.com', 'C@serta!23'))
    d = datetime.today() - timedelta(days=1)
    date = d.date().strftime('%Y%m%d')
    print date
    runURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/run-now"
    runData = {
        "job_id": 9316,
        "notebook_params": {
            "process_type": "match_tables",
            "date": "20170302"
           # "date": date
        }
    }
    res = requests.post(url=runURL, json=runData, auth=('production@aarp.com', 'C@serta!23'))
    if res.status_code == 200:
        print 'job launched successfully, will start monitoring'
        print res.json()
        sleep(10)
        monitorJob(str(res.json()['run_id']))

    else:
        raise ReferenceError(
        'The notebook id does not match the dobule_lake_click job name, please update job or notebook to match')
    if __name__ == "__main__":
        # stuff only to run when not called via 'import' here
        runArchiving()


def runActivity():
    clusterMetaData = createCluster()
    jobURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/get?job_id=9286"
    res = requests.get(url=jobURL, auth=('production@aarp.com', 'C@serta!23'))
    notebookDetails = res.json()
    notebookDetails['settings']['existing_cluster_id'] = clusterMetaData['cluster_id']
    if notebookDetails['settings']['name'] == 'doubleclick_lake_act':
        resetUrl = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/reset"
        resetData = {
            "job_id": 9286,
            "new_settings": notebookDetails['settings']
        }
        res = requests.post(url=resetUrl, json=resetData, auth=('production@aarp.com', 'C@serta!23'))
        d = datetime.today() - timedelta(days=1)
        date = d.date().strftime('%Y%m%d')
        print date
        runURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/run-now"
        runData = {
            "job_id": 9286,
            "notebook_params": {
                "path_inbound": "/mnt/landing/doubleclick/",
                "path_outbound": "/mnt/datalake/doubleclick",
                "process_type": "activity",
                "schema_file": "activity.csv",
                "schema_location": "/mnt/conf/schema/doubleclick/",
                "table_name": "doubleclick_activity",
                "date": "20170601"
                # "date": date
            }
        }
        res = requests.post(url=runURL, json=runData, auth=('production@aarp.com', 'C@serta!23'))
        if res.status_code == 200:
            print 'job launched successfully, will start monitoring'
            print res.json()
            sleep(10)
            monitorJob(str(res.json()['run_id']))
    else:
        raise ReferenceError(
            'The notebook id does not match the dobule_lake_click job name, please update job or notebook to match')

def runImpressions():
    clusterMetaData = createCluster()
    jobURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/get?job_id=9284"
    res = requests.get(url=jobURL, auth=('production@aarp.com', 'C@serta!23'))
    notebookDetails = res.json()
    notebookDetails['settings']['existing_cluster_id'] = clusterMetaData['cluster_id']
    if notebookDetails['settings']['name'] == 'doubleclick_lake_imp':
        resetUrl = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/reset"
        resetData = {
            "job_id": 9284,
            "new_settings": notebookDetails['settings']
        }
        res = requests.post(url=resetUrl, json=resetData, auth=('production@aarp.com', 'C@serta!23'))
        d = datetime.today() - timedelta(days=1)
        date = d.date().strftime('%Y%m%d')
        print date
        runURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/run-now"
        runData = {
            "job_id": 9284,
            "notebook_params": {
                "path_inbound": "/mnt/landing/doubleclick/",
                "path_outbound": "/mnt/datalake/doubleclick",
                "process_type": "impression",
                "schema_file": "impressions.csv",
                "schema_location": "/mnt/conf/schema/doubleclick/",
                "table_name": "doubleclick_impressions",
                "date": "20170601"
                # "date": date
            }
        }
        res = requests.post(url=runURL, json=runData, auth=('production@aarp.com', 'C@serta!23'))
        if res.status_code == 200:
            print 'job launched successfully, will start monitoring'
            print res.json()
            sleep(10)
            monitorJob(str(res.json()['run_id']))
    else:
        raise ReferenceError(
            'The notebook id does not match the dobule_lake_click job name, please update job or notebook to match')

def runClicks():
    clusterMetaData = createCluster()
    jobURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/get?job_id=9285"
    res = requests.get(url=jobURL, auth=('production@aarp.com','C@serta!23'))
    notebookDetails = res.json()
    notebookDetails['settings']['existing_cluster_id'] = clusterMetaData['cluster_id']
    if notebookDetails['settings']['name'] == 'doubleclick_lake_click':
        resetUrl = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/reset"
        resetData = {
            "job_id": 9285,
            "new_settings": notebookDetails['settings']
        }
        res = requests.post(url=resetUrl, json=resetData, auth=('production@aarp.com','C@serta!23'))
        d = datetime.today() - timedelta(days=1)
        date = d.date().strftime('%Y%m%d')
        print date
        runURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/run-now"
        runData = {
            "job_id": 9285,
            "notebook_params": {
                "path_inbound": "/mnt/landing/doubleclick/",
                "path_outbound": "/mnt/datalake/doubleclick",
                "process_type": "click",
                "schema_file": "clicks.csv",
                "schema_location": "/mnt/conf/schema/doubleclick/",
                "table_name": "doubleclick_click",
                "date": "20170601"
                #"date": date
            }
        }
        res = requests.post(url=runURL, json=runData, auth=('production@aarp.com','C@serta!23'))
        if res.status_code == 200:
            print 'job launched successfully, will start monitoring'
            print res.json()
            sleep(10)
            monitorJob(str(res.json()['run_id']))
    else:
        raise ReferenceError('The notebook id does not match the dobule_lake_click job name, please update job or notebook to match')
