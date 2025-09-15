import requests
import os
import pandas as pd
from pandas import json_normalize
from datetime import datetime

email = os.getenv("EMT_EMAIL")
password = os.getenv("EMT_PASSWORD")
stop_file = os.getenv("EMT_STOP_FILE")

def get_arrivals_for_stops(access_token, stops_file):
    # Obtener la fecha actual en formato YYYY-MM-DD
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"arrivals-{current_date}.xlsx" 

    stops_df = pd.read_excel(stops_file)
    print("Columnas disponibles en el archivo Excel:", stops_df.columns)  # Depuración

    all_arrivals = pd.DataFrame()

    headers = {
        "accessToken": access_token,
        "Content-Type": "application/json"
    }

    for _, row in stops_df.iterrows():
        try:
            # Verificar que las columnas necesarias existan
            if 'line' not in row or 'stops' not in row:
                print("Error: La fila no contiene las columnas necesarias ('line' o 'stops').")
                continue

            line = row['line']  # Línea de autobús
            stops = eval(row['stops'])  
            
            for stop in stops:
                stop_id = stop['stop']  # ID de la parada
                
                url = f"https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{stop_id}/arrives/{line}/"
                print(f"Consultando: {url}")
                
                body = {
                    "cultureInfo": "ES",
                    "Text_StopRequired_YN": "Y",
                    "Text_EstimationsRequired_YN": "Y",
                    "Text_IncidencesRequired_YN": "Y"
                }
                
                # POST
                response = requests.post(url, headers=headers, json=body, verify=False)
                
                if response.status_code == 200:
                    # Normalizar el JSON y agregarlo al DataFrame
                    payload = response.json()
                    arrivals_df = json_normalize(payload.get("data", []))
                    arrivals_df['stopId'] = stop_id  
                    arrivals_df['line'] = line 
                    
                    # Agregar columna de timestamp al inicio
                    arrivals_df.insert(0, "timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    
                    all_arrivals = pd.concat([all_arrivals, arrivals_df], ignore_index=True)
                else:
                    print(f"Error al consultar parada {stop_id}, línea {line}: {response.status_code} - {response.text}")
        except KeyError as e:
            print(f"Error: No se encontró la clave {e} en la fila.")
        except Exception as e:
            print(f"Error general al procesar la fila: {e}")

    # Guardar el DataFrame resultante en un archivo Excel
    try:
        existing_data = pd.read_excel(output_file)
        all_arrivals = pd.concat([existing_data, all_arrivals], ignore_index=True)
    except FileNotFoundError:
        pass

    all_arrivals.to_excel(output_file, index=False, engine="openpyxl")
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
            
            data = response.json().get("data", [])
            if data:
                access_token = data[0].get("accessToken", None)
                if access_token:
                    # Llamar a la función para obtener las llegadas
                    get_arrivals_for_stops(
                        access_token=access_token,
                        stops_file=stop_file
                    )
                else:
                    print("Error: No se encontró el accessToken en la respuesta.")
            else:
                print("Error: Respuesta de login no contiene datos válidos.")
        else:
            print(f"Error en la solicitud de login: {response.status_code} - {response.text}")
    except requests.exceptions.SSLError as e:
        print("Error SSL:", e)
    except Exception as e:
        print(f"Error general: {e}")

login_to_emtmadrid(email, password)
