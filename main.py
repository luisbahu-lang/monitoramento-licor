import os
import json
import re
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta

# CONFIGURAÇÕES
BASE_URL = "https://api.licor.cloud"
SOURCE_LINK = "https://www.licor.cloud/devices/6c53cc52-fb78-4318-a2c6-81b47ddb02eb/connected-scene/quick-chart/22127972-1"
API_TOKEN = os.getenv("LICOR_TOKEN")
STATION_HINT = "agropv"
MASTER_FILE = "historico_estacao.csv"

def api_get(path, params=None):
    url = f"{BASE_URL}{path}"
    r = session.get(url, params=params, timeout=60)
    if not r.ok: return None
    try: return r.json()
    except: return None

def get_devices_payload():
    for path in ["/v2/devices", "/v1/devices"]:
        payload = api_get(path)
        if payload: return payload
    return None

if __name__ == "__main__":
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {API_TOKEN}", "Accept": "application/json"})
    
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    end = now.strftime("%Y-%m-%d %H:%M:%S")

    try:
        payload = get_devices_payload()
        # Simplificando para pegar o primeiro ID disponível da estação
        # (Lógica interna para varrer os sensores do print)
        identifier = "22142456" # Serial da estação Agro PV do print
        
        data_payload = api_get("/v1/data", {"loggers": identifier, "start_date_time": start, "end_date_time": end})
        
        if data_payload and "data" in data_payload:
            df_new = pd.json_normalize(data_payload["data"])
            
            if not df_new.empty:
                # Se o arquivo mestre já existe, junta (append) os dados novos
                if os.path.exists(MASTER_FILE):
                    df_old = pd.read_csv(MASTER_FILE)
                    df_final = pd.concat([df_old, df_new]).drop_duplicates()
                else:
                    df_final = df_new
                
                df_final.to_csv(MASTER_FILE, index=False, encoding="utf-8-sig")
                print(f"Dados atualizados no arquivo mestre: {MASTER_FILE}")
    except Exception as e:
        print(f"Erro na coleta: {e}")
