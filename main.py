import os
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

# --- CONFIGURAÇÕES ---
BASE_URL = "https://api.licor.cloud"
API_TOKEN = os.getenv("LICOR_TOKEN")
STATION_ID = "22142456" 
MASTER_FILE = "historico_estacao.csv"

def api_get(path, params=None):
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {API_TOKEN}", "Accept": "application/json"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        return r.json() if r.ok else None
    except:
        return None

if __name__ == "__main__":
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    end = now.strftime("%Y-%m-%d %H:%M:%S")

    data_payload = api_get("/v1/data", {"loggers": STATION_ID, "start_date_time": start, "end_date_time": end})

    if data_payload and "data" in data_payload:
        df_raw = pd.json_normalize(data_payload["data"])
        
        if not df_raw.empty:
            # Pivot para organizar colunas
            df_new = df_raw.pivot_table(
                index='timestamp', 
                columns='sensor_measurement_type', 
                values='value',
                aggfunc='first'
            ).reset_index()

            df_new = df_new.rename(columns={'timestamp': 'Data_Hora'})
            
            # Garante que os números sejam decimais limpos
            for col in df_new.columns:
                if col != 'Data_Hora':
                    df_new[col] = pd.to_numeric(df_new[col], errors='coerce').round(2)

            # Salva o arquivo (Como deletamos o anterior, ele criará um novo limpo)
            df_new.to_csv(MASTER_FILE, index=False, encoding="utf-8-sig")
            print("Novo arquivo criado com sucesso!")
