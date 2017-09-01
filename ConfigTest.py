import json


with open('./Config.properties') as json_data:
    data=json.load(json_data)
    airflow_environment=data['Airflow_Config']['Environment']

    if (airflow_environment=="DEV"):
        print("printing Dev parameters")
        data['Airflow_DEV']['fcom']['datalake']
        print (submiturl)
    else:
        print("printing  production  parameters")
        submiturl = data['Airflow_PRODUCTION']['submiturl']
        print (submiturl)
