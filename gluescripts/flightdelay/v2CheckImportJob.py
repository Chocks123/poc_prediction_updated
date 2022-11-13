import sys
import boto3
import time
from awsglue.utils import getResolvedOptions

session = boto3.Session()
forecast = session.client(service_name='forecast') 
glue_client = session.client(service_name='glue')

args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
workflowName = args['WORKFLOW_NAME']
workflow = glue_client.get_workflow(Name=workflowName)
workflowRunId = args['WORKFLOW_RUN_ID']
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

stocks_import_ds_arn = workflow_params['targetTimeSeriesDataset']
# initialise import job status for while loop
stocksDataImportStatus = forecast.describe_dataset(DatasetArn=stocks_import_ds_arn)['Status']

while True:    
    if (stocksDataImportStatus == 'ACTIVE'):
        break
    elif (stocksDataImportStatus == 'CREATE_FAILED'):
        raise NameError('Import create failed')
    stocksDataImportStatus = forecast.describe_dataset(DatasetArn=stocks_import_ds_arn)['Status']
    time.sleep(10)

print ('Stocks Data Import status is: ' + stocksDataImportStatus)
