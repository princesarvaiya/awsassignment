import json
import boto3
import time

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client("s3")
    data = json.loads(event["Records"][0]["body"])
    
    epoch_time = time.time()
    epoch_time_string = str(epoch_time)
    epoch_time_string = epoch_time_string.replace('.','')
    epoch_time_string = 'data_' + epoch_time_string +'.json'
    
    s3.put_object(Bucket="lambda-store-json",Key=epoch_time_string,Body=json.dumps(data))
