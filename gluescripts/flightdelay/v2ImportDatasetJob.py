import sys
import boto3
import datetime
import random
import os
import time
from datetime import datetime
import pytz
from awsglue.utils import getResolvedOptions

tz_London = pytz.timezone('Europe/London')
datetime_London = datetime.now(tz_London)
datestr = str(datetime_London.strftime("%Y%m%d%H"))

session = boto3.Session()
forecast = session.client(service_name='forecast') 
glue_client = boto3.client("glue")
args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
workflowName = args['WORKFLOW_NAME']
workflow = glue_client.get_workflow(Name=workflowName)
workflowRunId = args['WORKFLOW_RUN_ID']
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]
                                        
iam = session.resource('iam')

# In our dataset, the timeseries values are recorded every day
DATASET_FREQUENCY = workflow_params['dataset_frequency'] 
TIMESTAMP_FORMAT = workflow_params['timestamp_format']

rnd = str(random.getrandbits(12))
project = 'stock_forecast_' + workflow_params['exchange_frequency_pair'] + datestr
datasetName = project + '_ds'
datasetGroupName = project + '_dsg'
bucket_name = workflow_params['inBucket']
stocks_file = workflow_params['incomingStockDatafile']
role = iam.Role('MLOpsUserRole')
s3DataPath = 's3://' + bucket_name + '/' + stocks_file

print('stocks_file is: ' + stocks_file)
print('project is: ' + project)
print('DATASET_FREQUENCY is : ' + DATASET_FREQUENCY)
print('TIMESTAMP_FORMAT is : ' + TIMESTAMP_FORMAT)

create_dataset_group_response = forecast.create_dataset_group(DatasetGroupName=datasetGroupName,
                                                            Domain="CUSTOM",
                                                            )
datasetGroupArn = create_dataset_group_response['DatasetGroupArn']
workflow_params['datasetGroupArn'] = datasetGroupArn
workflow_params['projectName'] = project

glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

def start_stock_import_job(s3DataPath, datasetName, datasetGroupArn, role_arn):
    # Specify the schema of your dataset here. Make sure the order of columns matches the raw data files.
    schema = {
    "Attributes": [
        {
            "AttributeName": "item_id",
            "AttributeType": "string"
        },
        {
            "AttributeName": "timestamp",
            "AttributeType": "timestamp"
        },
        {
            "AttributeName": "target_value",
            "AttributeType": "float"
        }
    ]
    }

    response = forecast.create_dataset(
                    Domain="CUSTOM",
                    DatasetType='TARGET_TIME_SERIES',
                    DatasetName=datasetName,
                    DataFrequency=DATASET_FREQUENCY, 
                    Schema = schema)

    TargetdatasetArn = response['DatasetArn']
    workflow_params['targetTimeSeriesDataset'] = TargetdatasetArn
    print('targetTimeSeriesDataset: '+workflow_params['targetTimeSeriesDataset'])
    updateDatasetResponse = forecast.update_dataset_group(DatasetGroupArn=datasetGroupArn, DatasetArns=[TargetdatasetArn])

    # stocks dataset import job
    datasetImportJobName = 'STOCK_DSIMPORT_JOB_TARGET'+workflow_params['exchange_frequency_pair']
    ds_import_job_response=forecast.create_dataset_import_job(DatasetImportJobName=datasetImportJobName,
                                                            DatasetArn=TargetdatasetArn,
                                                            DataSource= {
                                                                "S3Config" : {
                                                                    "Path": s3DataPath,
                                                                    "RoleArn": role_arn
                                                                } 
                                                            },
                                                            TimestampFormat=TIMESTAMP_FORMAT
                                                            )

    ds_import_job_arn=ds_import_job_response['DatasetImportJobArn']

    workflow_params['stocksImportJobRunId'] = ds_import_job_arn
    
    return {
    "importJobArn": ds_import_job_arn,
    "datasetGroupArn": datasetGroupArn,
    "stocksDatasetArn": TargetdatasetArn
    }

stock_import_result = start_stock_import_job(s3DataPath, datasetName, datasetGroupArn, role.arn)

glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('output Dataset Group Arn is: ' + workflow_params['datasetGroupArn'])
print('s3DataPath is: ' + s3DataPath)
print('role_arn is: ' + role.arn)
