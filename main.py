import os
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

# --- CONFIGURAÇÕES ---
BASE_URL = "https://api.licor.cloud"
API_TOKEN = os.getenv("LICOR_TOKEN")
STATION_ID = "22142456" 
MASTER_FILE = "historico_estacao.csv"

# DICIONÁRIO DE TRADUÇÃO (Baseado no seu print da LI-COR)
# Se aparecerem novos códigos no seu CSV, podemos adicioná-los aqui depois
MAPA_SENSORES = {
    'timestamp': 'Data_Hora',
    'c_22127972_1': 'Chuva_mm',
    'c_22127972_2': 'Chuva_Acumulada_mm',
    'c_22146362_1': 'Velocidade_Vento_ms',
    'c_22146362_2': 'Rajada_Vento_ms',
    'c_22146362_3': 'Direcao_Vento_graus',
    'c_22122567_1': 'Temperatura_C',
    'c_22122567_2': 'Umidade_Relativa_perc',
    'c_22122567_3': 'Ponto_Orvalho_C',
    'c_22122449_1': 'Radiacao_Solar_Wm2',
    'c_22116417_1': 'PAR_uE',
    'c_22166457_1': 'Umidade_Solo_VWC_1',
    'c_22166481_1': 'Umidade_Solo_VWC_2',
    'c_22154209_1': 'Umidade_Solo_VWC_3'
}

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
        df_new = pd.json_normalize(data_payload["data"])
        
        if not df_new.empty:
            # APLICA A TRADUÇÃO DAS COLUNAS
            df_new = df_new.rename(columns=MAPA_SENSORES)
            
            # Garante que a Data_Hora seja a primeira coluna
            cols = ['Data_Hora'] + [c for c in df_new.columns if c != 'Data_Hora']
            df_new = df_new[cols]

            if os.path.exists(MASTER_FILE):
                df_old = pd.read_csv(MASTER_FILE)
                df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Data_Hora'])
            else:
                df_final = df_new
            
            df_final.to_csv(MASTER_FILE, index=False, encoding="utf-8-sig")
            print(f"Dados traduzidos e salvos!")
