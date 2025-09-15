import requests
import pandas as pd
import os
from pandas import json_normalize

email = os.getenv("EMT_EMAIL")
password = os.getenv("EMT_PASSWORD")

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
                header = {
                    "accessToken": access_token
                }
                print(header)
                
                response = requests.get(
                    "https://openapi.emtmadrid.es/v2/transport/busemtmad/lines/info/",
                    headers=header,
                    verify=False
                )
                
                if response.status_code == 200:
                    # Normalizar el JSON
                    payload = response.json()
                    df = json_normalize(payload.get("data", [])) 
                    
                    # Guardar el DataFrame en un archivo Excel
                    output_file = "bus_lines.xlsx"
                    df.to_excel(output_file, index=False, engine="openpyxl")
                    print(f"Datos guardados en el archivo {output_file}")
                else:
                    print(f"Error en la solicitud de paradas: {response.status_code} - {response.text}")
            else:
                print("Error: No se encontró el accessToken en la respuesta.")
        else:
            print(f"Error en la solicitud de login: {response.status_code} - {response.text}")
    except requests.exceptions.SSLError as e:
        print("Error SSL:", e)
    except Exception as e:
        print("Error general:", e)

login_to_emtmadrid(email, password)
