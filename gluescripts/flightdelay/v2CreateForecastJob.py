import sys
import boto3
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

project = workflow_params['projectName']
forecastName= project + '_aml_forecast'
predictorArn = workflow_params['predictorArn']
    
create_forecast_response=forecast.create_forecast(ForecastName=forecastName,
                                                  PredictorArn=predictorArn)
forecastArn = create_forecast_response['ForecastArn']

workflow_params['forecastArn'] = forecastArn
workflow_params['forecastName'] = forecastName
glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('Forecast Arn is: ' + workflow_params['forecastArn'])
