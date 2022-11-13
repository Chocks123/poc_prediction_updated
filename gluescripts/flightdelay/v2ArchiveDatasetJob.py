import sys
import boto3
import datetime
import random
import os
import time
from datetime import datetime
import pytz
from awsglue.utils import getResolvedOptions

session = boto3.Session()
glue_client = session.client(service_name='glue')

args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
workflowName = args['WORKFLOW_NAME']
workflow = glue_client.get_workflow(Name=workflowName)
workflowRunId = args['WORKFLOW_RUN_ID']
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]
                                        
tz_London = pytz.timezone('Europe/London')
datetime_London = datetime.now(tz_London)
datestr = str(datetime_London.strftime("%Y%m%d%H"))

project = workflow_params['projectName']
bucketname = workflow_params['inBucket']
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucketname)

##standard values
source = "stock/"
target = "processed/"+project

for obj in my_bucket.objects.filter(Prefix=source):
    source_filename = (obj.key).split('/')[-1]
    copy_source = {
        'Bucket': bucketname,
        'Key': obj.key
    }
    target_filename = "{}/{}".format(target, source_filename)
    s3.meta.client.copy(copy_source, bucketname, target_filename)
    # Uncomment the line below if you wish the delete the original source file
    # s3.Object(bucketname, obj.key).delete()