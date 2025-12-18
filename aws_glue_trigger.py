import boto3
import json
glueclient = boto3.client('glue')

def lambda_handler(event, context):
        glueclient.start_job_name(JobName='trigger-glue')
        return {
        'statusCode': 200,
        'body': json.dumps('Job completed')
    }
