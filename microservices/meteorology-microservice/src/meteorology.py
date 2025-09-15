"""
    Libraries
"""
import json
import time
import sys
import boto3
import requests
import os
import datetime
from botocore.exceptions import ClientError
from botocore.config import Config
"""
    Config
"""
"""
    Config
"""
config = Config(
    retries = dict(
        max_attempts = 10
    )
)
"""
    Variables
"""
SECRET_NAME = os.getenv('SECRET_NAME')
KINESIS_STREAM_NAME = os.getenv('KINESIS_STREAM_NAME')
CIUDAD = "Madrid"
URL = "https://api.weatherapi.com/v1/current.json"
session = boto3.session.Session()
secret_client = session.client("secretsmanager", config=config)
kinesis_client = session.client("kinesis", config=config)
"""
    Functions
"""
def obtener_clima(api_key, last_updated):

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    params = {
        "key": api_key,
        "q": CIUDAD,
        "dt": today,
        "aqi": "yes"  
    }

    resp = requests.get(URL, params=params)
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return last_updated

    datos = resp.json()
    clima = datos.get('current')
    if not clima:
        print("La respuesta no contiene 'current'")
        return

    send_to_kinesis(clima)
    return

def get_api_key_from_secret(secret_name: str) -> str:
    """
    Recupera el secreto y devuelve el campo 'api_key'.
    """
    try:
        resp = secret_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(resp['SecretString'])
        return secret['api_key']
    except ClientError as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        sys.exit(1)

def send_to_kinesis(record: dict):
    """
    Envía el JSON 'record' a Kinesis.
    Usa 'last_updated_epoch' como PartitionKey si está disponible.
    """
    data = json.dumps(record).encode('utf-8')
    part_key = str(record.get('last_updated_epoch', int(time.time())))
    try:
        resp = kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=data,
            PartitionKey=part_key
        )
        print(f"Enviado a Kinesis: shard={resp['ShardId']} seq={resp['SequenceNumber']}")
    except ClientError as e:
        print(f"Error sending to Kinesis: {e}")
"""
    Main
"""
def main():
    API_KEY = get_api_key_from_secret(SECRET_NAME)
    last_updated = None
            
    while True:
        last_updated = obtener_clima(API_KEY, last_updated)
        time.sleep(1800)

if __name__ == "__main__":
    main()
