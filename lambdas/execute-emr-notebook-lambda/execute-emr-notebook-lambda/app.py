import boto3
import json
import os
from urllib.parse import unquote

EMR_APP_ID = os.environ["EMR_APP_ID"]
EMR_ROLE_ARN = os.environ["EMR_RUNTIME_ROLE_ARN"]
JOB_SCRIPT_S3 = os.environ["JOB_SCRIPT_S3"]
WAREHOUSE_S3 = os.environ["WAREHOUSE_S3"]
DB_NAME = os.environ.get("DB_NAME")
S3_LOGS = os.environ.get("S3_LOGS")

emr = boto3.client("emr-serverless")

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        key = unquote(key)
        input_s3 = f"s3://{bucket}/{key}"

        job_driver = {
            "sparkSubmit": {
                "entryPoint": JOB_SCRIPT_S3,
                "entryPointArguments": [
                    "--input", input_s3,
                    "--warehouse", WAREHOUSE_S3,
                    "--db", DB_NAME
                ],
                "sparkSubmitParameters": " ".join([
                      "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
                      f"--conf spark.sql.catalog.glue_catalog.warehouse={WAREHOUSE_S3}",
                      "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
                      "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
                      "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                      "--conf spark.sql.sources.partitionOverwriteMode=dynamic",

                      # Recursos (ajusta si hace falta)
                      "--conf spark.executor.instances=2",
                      "--conf spark.executor.memory=4g",
                      "--conf spark.executor.cores=2",
                      "--conf spark.driver.memory=2g",

                      #  SIN --packages. Usamos el bundle en S3:
                      "--jars s3://config-tfm-dgv-mbdde-bucket/jars/spark-excel/spark-excel-bundle-3.5.1_0.20.4-all.jar"
                ])
            }
        }

        resp = emr.start_job_run(
            applicationId=EMR_APP_ID,
            executionRoleArn=EMR_ROLE_ARN,
            name=f"{os.path.basename(key)}",
            jobDriver=job_driver,
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{S3_LOGS}/emr-logs/"
                    }
                }
            }
        )

        print(resp)
    except Exception as e:
        print(f"Error al iniciar el job: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error al iniciar el job: {str(e)}"
        }
