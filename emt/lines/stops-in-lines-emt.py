import requests
import pandas as pd
import os
from pandas import json_normalize
from datetime import datetime

email = os.getenv("EMT_EMAIL")
password = os.getenv("EMT_PASSWORD")

def get_stops_for_lines(access_token, input_file):
    # Obtener la fecha actual en formato yyyy-mm-dd
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"bus_stops_{current_date}.xlsx" 

    bus_lines_df = pd.read_excel(input_file)
    
    all_stops = pd.DataFrame()

    headers = {
        "accessToken": access_token
    }

    for line in bus_lines_df['line']:
        for stop_id in line:  
            url = f"https://openapi.emtmadrid.es/v1/transport/busemtmad/lines/{line}/stops/{stop_id}/"
            print(f"Consultando: {url}")
            
            # GET
            response = requests.get(url, headers=headers, verify=False)
            
            if response.status_code == 200:
                # Normalizar el JSON 
                payload = response.json()
                stops_df = json_normalize(payload.get("data", []))
                stops_df['line'] = line  
                stops_df['stop_id'] = stop_id  
                
                stops_df.insert(0, "timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                
                all_stops = pd.concat([all_stops, stops_df], ignore_index=True)
            else:
                print(f"Error al consultar línea {line}, stop_id {stop_id}: {response.status_code} - {response.text}")

    # Guardar el DataFrame resultante en un archivo Excel
    try:
        existing_data = pd.read_excel(output_file)
        all_stops = pd.concat([existing_data, all_stops], ignore_index=True)
    except FileNotFoundError:
        pass

    all_stops.to_excel(output_file, index=False, engine="openpyxl")
    print(f"Datos guardados en el archivo {output_file}")


def login_to_emtmadrid(email, password):
    url = "https://datos.emtmadrid.es/v3/mobilitylabs/user/login/"
    headers = {
        "Content-Type": "application/json",
        "email": email,
        "password": password
    }

    try:
        response = requests.get(url, headers=headers, verify=False)  # verify=False para evitar problemas SSL
        if response.status_code == 200:
            print("Login exitoso:", response.json())
            
            access_token = response.json().get("data", [{}])[0].get("accessToken", None)
            
            if access_token:
                get_stops_for_lines(
                    access_token=access_token,
                    input_file="bus_lines.xlsx"
                )
            else:
                print("Error: No se encontró el accessToken en la respuesta.")
        else:
            print(f"Error en la solicitud de login: {response.status_code} - {response.text}")
    except requests.exceptions.SSLError as e:
        print("Error SSL:", e)
    except Exception as e:
        print("Error general:", e)

# Ejecutar el script
login_to_emtmadrid(email, password)