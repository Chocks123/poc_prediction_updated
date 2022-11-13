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

forecastExportArn = workflow_params['forecastExportArn']
# initialise forecast job status for while loop
forecastExportStatus = forecast.describe_forecast_export_job(ForecastExportJobArn=forecastExportArn)['Status']

while (forecastExportStatus != 'ACTIVE'):
    forecastExportStatus = forecast.describe_forecast_export_job(ForecastExportJobArn=forecastExportArn)['Status']
    if (forecastExportStatus == 'CREATE_FAILED'):
        raise NameError('Forecast export failed')
    time.sleep(10)

print ('Forecast export status is: ' + forecastExportStatus)
