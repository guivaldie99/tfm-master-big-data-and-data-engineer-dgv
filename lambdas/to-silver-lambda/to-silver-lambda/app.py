import boto3
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, TimestampType, StructType, ListType

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Leer variables de entorno
    database_name = os.getenv("DATABASE_NAME")
    table_name = os.getenv("TABLE_NAME")
    schema_definition = json.loads(os.getenv("SCHEMA"))  # Leer el esquema desde las variables de entorno
    
    # Procesar cada archivo del evento
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        # Descargar el archivo desde S3
        file_path = f"/tmp/{object_key.split('/')[-1]}"
        s3_client.download_file(bucket_name, object_key, file_path)
        
        # Leer el archivo Excel
        df = pd.read_excel(file_path)
        
        # Validar el esquema
        expected_columns = [col["name"] for col in schema_definition]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError(f"El archivo {object_key} no cumple con el esquema esperado.")
        
        # Guardar los datos en una tabla Iceberg
        save_to_iceberg(df, bucket_name, database_name, table_name, schema_definition)

def save_to_iceberg(df, bucket_name, database, table, schema_definition):
    # Convertir el esquema dinámicamente desde la definición
    iceberg_schema = Schema(
        [(col["name"], eval(col["type"])) for col in schema_definition]
    )
    
    # Ruta de la tabla Iceberg
    table_path = f"s3://{bucket_name}/silver/{database}/{table}"
    
    # Crear o actualizar la tabla Iceberg
    catalog = load_catalog("s3")  # Configura tu catálogo Iceberg
    iceberg_table = catalog.create_table(f"{database}.{table}", iceberg_schema, table_path)
    
    # Escribir los datos en la tabla Iceberg
    table = ds.dataset(table_path, format="parquet")
    table.write(pa.Table.from_pandas(df))
