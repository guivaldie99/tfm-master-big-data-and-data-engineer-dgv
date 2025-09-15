import base64
import json
import os

import boto3

# Name or ARN of the Lambda that writes to Iceberg
WRITER_FUNCTION_NAME = os.environ['WRITER_FUNCTION_NAME']

lambda_client = boto3.client('lambda')

# Define your key‐renaming map here:
KEY_MAP = {
    "last_updated_epoch": "ts_epoch",
    "last_updated":       "timestamp",
    "temp_c":             "temperature_celsius",
    "temp_f":             "temperature_fahrenheit",
    "is_day":             "day_flag",
    "condition":          "weather_condition",
    "wind_mph":           "wind_mph",
    "wind_kph":           "wind_kph",
    "wind_degree":        "wind_degree",
    "wind_dir":           "wind_direction",
    "pressure_mb":        "pressure_mbar",
    "pressure_in":        "pressure_inhg",
    "precip_mm":          "precip_mm",
    "precip_in":          "precip_in",
    "humidity":           "humidity",
    "cloud":              "cloud_percent",
    "feelslike_c":        "feelslike_celsius",
    "feelslike_f":        "feelslike_fahrenheit",
    "windchill_c":        "windchill_celsius",
    "windchill_f":        "windchill_fahrenheit",
    "heatindex_c":        "heatindex_celsius",
    "heatindex_f":        "heatindex_fahrenheit",
    "dewpoint_c":         "dewpoint_celsius",
    "dewpoint_f":         "dewpoint_fahrenheit",
    "vis_km":             "visibility_km",
    "vis_miles":          "visibility_miles",
    "uv":                 "uv_index",
    "gust_mph":           "gust_mph",
    "gust_kph":           "gust_kph",
    "air_quality":        "air_quality"
}

def lambda_handler(event, context):
    output = []
    
    for record in event['records']:
        # 1. Decode incoming Firehose record
        payload = base64.b64decode(record['data'])
        try:
            obj = json.loads(payload)
        except Exception:
            # If not valid JSON, simply pass it through
            output.append({
                'recordId': record['recordId'],
                'result': 'DeliveryFailed',
                'data': record['data']
            })
            continue

        # 2. Transform only the "current" block
        current = obj.get('current', {})
        transformed = {}
        for old_key, new_key in KEY_MAP.items():
            if old_key in current:
                transformed[new_key] = current[old_key]

        # 3. (Optional) embed metadata like location or timestamp
        transformed['ingest_time'] = int(context.aws_request_id[:8], 16)  # example
        transformed['raw_partition_key'] = record.get('partitionKey', '')

        # 4. Invoke the writer Lambda asynchronously
        try:
            lambda_client.invoke(
                FunctionName=WRITER_FUNCTION_NAME,
                InvocationType='Event',
                Payload=json.dumps(transformed).encode('utf-8')
            )
        except Exception as e:
            # log and continue
            print(f"Failed to invoke writer: {e}")

        # 5. Return the (optionally) transformed record for Firehose
        out_data = base64.b64encode(json.dumps(transformed).encode('utf-8')).decode('utf-8')
        output.append({
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': out_data
        })

    return {'records': output}
