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

forecastHorizon = workflow_params['forecast_horizon'] 
forecastHorizon = int(forecastHorizon)
datasetGroupArn = workflow_params['datasetGroupArn']
project = workflow_params['projectName']

backtestWindows = workflow_params['backtest_windows']
backtestWindows = int(backtestWindows)

predictorName= project + '_aml'

print('datasetGroupArn imported is: ' + datasetGroupArn)

create_predictor_response=forecast.create_predictor(PredictorName=predictorName,
                                                ForecastHorizon=forecastHorizon,
                                                PerformAutoML= True,
                                                EvaluationParameters= {"NumberOfBacktestWindows": backtestWindows, 
                                                                "BackTestWindowOffset": forecastHorizon},
                                                InputDataConfig= {"DatasetGroupArn": datasetGroupArn},
                                                FeaturizationConfig= {"ForecastFrequency": workflow_params['dataset_frequency'],
                                                "Featurizations": [{"AttributeName": "target_value",
                                                "FeaturizationPipeline": [{"FeaturizationMethodName": "filling",
                                                "FeaturizationMethodParameters": {"frontfill": "none",
                                                "aggregation": workflow_params['aggregation_value'],
                                                "backfill": workflow_params['back_fill'],
                                                "middlefill": workflow_params['middle_fill']}
                                                }]
                                                }]
                                                })
predictorArn=create_predictor_response['PredictorArn']

workflow_params['predictorArn'] = predictorArn
glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('output Predictor Arn is: ' + workflow_params['predictorArn'])
