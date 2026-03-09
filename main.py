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
    # Pega os dados da última hora
    start = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    end = now.strftime("%Y-%m-%d %H:%M:%S")

    data_payload = api_get("/v1/data", {"loggers": STATION_ID, "start_date_time": start, "end_date_time": end})

    if data_payload and "data" in data_payload:
        df_raw = pd.json_normalize(data_payload["data"])
        
        if not df_raw.empty:
            # 1. Transformar linhas em colunas (Pivot)
            df_new = df_raw.pivot_table(
                index='timestamp', 
                columns='sensor_measurement_type', 
                values='value',
                aggfunc='first'
            ).reset_index()

            # 2. Renomear e formatar números
            df_new = df_new.rename(columns={'timestamp': 'Data_Hora'})
            
            for col in df_new.columns:
                if col != 'Data_Hora':
                    # Converte para número e arredonda para 2 casas decimais
                    df_new[col] = pd.to_numeric(df_new[col], errors='coerce').round(2)

            # 3. Gerenciar o arquivo Histórico
            if os.path.exists(MASTER_FILE):
                try:
                    # Tenta ler com o separador correto
                    df_old = pd.read_csv(MASTER_FILE, sep=';')
                    df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['Data_Hora'])
                except:
                    # Se der erro (arquivo antigo sujo), começa um novo
                    df_final = df_new
            else:
                df_final = df_new
            
            # 4. SALVAR com ponto e vírgula (Crucial para Google Sheets Uruguai)
            df_final.to_csv(MASTER_FILE, index=False, sep=';', encoding="utf-8-sig")
            print("Processamento concluído: Arquivo salvo com separador ';'")
