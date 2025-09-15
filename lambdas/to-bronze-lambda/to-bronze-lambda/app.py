import boto3
import os
from urllib.parse import unquote  # Importar la función para decodificar

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Obtener información del evento
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        encoded_source_key = event['Records'][0]['s3']['object']['key']
        
        # Decodificar la clave del objeto
        source_key = unquote(encoded_source_key)
        print(f"Clave original codificada: {encoded_source_key}")
        print(f"Clave decodificada: {source_key}")
        
        # Definir el nuevo path (destino) dentro del mismo bucket
        destination_key = source_key.replace("raw/", "bronze/")
        
        # Mover el archivo (copiar y luego eliminar el original)
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=destination_key
        )
        
        print(f"Archivo movido de {source_key} a {destination_key} en el bucket {bucket_name}")
        return {
            'statusCode': 200,
            'body': f"Archivo movido de {source_key} a {destination_key} en el bucket {bucket_name}"
        }
    except Exception as e:
        print(f"Error al mover el archivo: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error al mover el archivo: {str(e)}"
        }
