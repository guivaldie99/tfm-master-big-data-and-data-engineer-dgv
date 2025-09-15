import os
import json
import time
import boto3
from botocore.exceptions import ClientError

GLUE       = boto3.client("glue")
ATHENA     = boto3.client("athena")

# ==== ENV VARS (define estas en CFN) ====
ACCOUNT_ID            = os.environ["AWS_ACCOUNT_ID"]                    # 123456789012
REGION                = os.environ["AWS_REGION_ID"]                        # eu-west-1 (Lambda ya la expone, pero puedes fijarla)
DATALAKE_BUCKET       = os.environ["DATALAKE_BUCKET"]                   # p.ej. obdl-xxx
DB_NAME               = os.environ["ICEBERG_DB_NAME"]                   # p.ej. dev_meteo_db
TABLE_NAME            = os.environ["ICEBERG_TABLE_NAME"]                # p.ej. meteorology
WAREHOUSE_PATH        = os.environ["ICEBERG_WAREHOUSE_PATH"]            # s3://<bucket>/data/
ATHENA_WORKGROUP      = os.environ["ATHENA_WORKGROUP"]                  # p.ej. primary (o uno dedicado)
ATHENA_OUTPUT_S3      = os.environ["ATHENA_OUTPUT_S3"]                  # s3://<bucket-athena-query-results>/prefix/

# Ruta final para la tabla (coherente con tu DB/tabla)
def table_s3_location() -> str:
    # ej: s3://<bucket>/data/<db>/<table>/
    return f"{WAREHOUSE_PATH.rstrip('/')}/{DB_NAME}/{TABLE_NAME}/"

ICEBERG_DDL = f"""
CREATE TABLE IF NOT EXISTS "{DB_NAME}"."{TABLE_NAME}" (
  ts_epoch                bigint,
  "timestamp"             timestamp,
  temperature_celsius     double,
  temperature_fahrenheit  double,
  day_flag                int,
  weather_condition       varchar,
  wind_mph                double,
  wind_kph                double,
  wind_degree             int,
  wind_direction          varchar,
  pressure_mbar           double,
  pressure_inhg           double,
  precip_mm               double,
  precip_in               double,
  humidity                int,
  cloud_percent           int,
  feelslike_celsius       double,
  feelslike_fahrenheit    double,
  windchill_celsius       double,
  windchill_fahrenheit    double,
  heatindex_celsius       double,
  heatindex_fahrenheit    double,
  dewpoint_celsius        double,
  dewpoint_fahrenheit     double,
  visibility_km           double,
  visibility_miles        double,
  uv_index                double,
  gust_mph                double,
  gust_kph                double,
  air_quality             varchar
)
PARTITIONED BY (year int, month int, day int)
LOCATION '{table_s3_location()}'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format'     = 'parquet'
)
"""

def ensure_table_exists():
    """Si la tabla no existe en Glue, la crea mediante Athena DDL."""
    try:
        GLUE.get_table(DatabaseName=DB_NAME, Name=TABLE_NAME)
        # Existe: nada que hacer
        return {"created": False}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code != "EntityNotFoundException":
            raise  # otro error real

    # No existe -> crear vía Athena
    qexec = ATHENA.start_query_execution(
        QueryString=ICEBERG_DDL,
        QueryExecutionContext={"Database": DB_NAME},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_S3},
    )
    qid = qexec["QueryExecutionId"]

    # Espera simple (polling) hasta que Athena termine
    while True:
        resp = ATHENA.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena DDL failed: {state} - {reason}")

    return {"created": True, "query_id": qid}

def lambda_handler(event, context):
    """
    Esta Lambda se invoca antes de insertar datos.
    Sólo asegura que la tabla Iceberg exista en el catálogo Glue.
    """
    result = ensure_table_exists()
    return {"status": "ok", **result}
