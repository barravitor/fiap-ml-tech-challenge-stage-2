from dotenv import load_dotenv
import os
import json
import boto3

load_dotenv(override=True)

def lambda_handler(event, context):
    glue_client = boto3.client('glue')

    response = glue_client.start_job_run(
        JobName=os.getenv("AWS_GLUE_JOB_NAME")
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f"Glue job started: {response['JobRunId']}")
    }
