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

predictorArn = workflow_params['predictorArn']
# initialise predictor job status for while loop
predictorStatus = forecast.describe_predictor(PredictorArn=predictorArn)['Status']

while (predictorStatus != 'ACTIVE'):
    predictorStatus = forecast.describe_predictor(PredictorArn=predictorArn)['Status']
    if (predictorStatus == 'CREATE_FAILED'):
        raise NameError('Predictor create failed')
    time.sleep(10)

print ('Predictor status is: ' + predictorStatus)
