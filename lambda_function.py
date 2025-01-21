from dotenv import load_dotenv
import os
import http.client
import boto3
import pandas as pd
from datetime import datetime
import json

load_dotenv(override=True)

def clean_and_convert(value, target_type):
    if isinstance(value, str):
        value = value.replace('.', '').replace(',', '.')
    if target_type == 'int':
        return int(float(value))
    elif target_type == 'float':
        return float(value)

    return value

def Request():
    conn = http.client.HTTPSConnection(os.getenv("B3_WEBSITE_URL"))

    conn.request("GET", os.getenv("B3_WEBSITE_PARAMS"))

    response = conn.getresponse()

    if response.status != 200:
        print(f"Error accessing the B3 API: {response.status_code}")
        return

    body = response.read().decode()

    data = json.loads(body)

    if "results" in data:
        results = data["results"]
    else:
        print("'results' not found in returned JSON.")

    conn.close()

    return results

def SaveS3File(parquet_file_name: str):
    s3 = boto3.client('s3')

    try:
        s3.upload_file(f"/tmp/{parquet_file_name}", "fiap-ml-tech-challenge", f"raw/{parquet_file_name}")
    except Exception as e:
        return {'statusCode': 500, 'body': f"Error uploading to S3: {str(e)}"}

def lambda_handler(event, context):
    results = Request()

    if not results:
        return {'statusCode': 404, 'body': 'Data not found!'}

    df = pd.DataFrame(results)

    df['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
    df['theoricalQty'] = df['theoricalQty'].apply(clean_and_convert, target_type='int')
    df['part'] = df['part'].apply(clean_and_convert, target_type='float')
    df['partAcum'] = df['partAcum'].apply(clean_and_convert, target_type='float')

    parquet_file_name = "Bovespa.parquet"
    df.to_parquet(f"/tmp/{parquet_file_name}", engine="fastparquet", index=False)

    SaveS3File(parquet_file_name)

    return {'statusCode': 200, 'body': 'Data saved successfully!'}

lambda_handler(0, 0)
