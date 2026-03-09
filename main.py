import os
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

# --- CONFIGURAÇÕES DO ENGENHEIRO ---
BASE_URL = "https://api.licor.cloud"
API_TOKEN = os.getenv("LICOR_TOKEN")
STATION_ID = "22142456" # ID da sua estação Agro PV
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
    # 1. Define a janela de tempo (última hora)
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    end = now.strftime("%Y-%m-%d %H:%M:%S")

    print(f"Iniciando coleta: {start} até {end}")

    # 2. Busca os dados de TODOS os sensores
    # O endpoint /v1/data retorna todos os canais disponíveis para o ID
    data_payload = api_get("/v1/data", {
        "loggers": STATION_ID, 
        "start_date_time": start, 
        "end_date_time": end
    })

    if data_payload and "data" in data_payload:
        df_new = pd.json_normalize(data_payload["data"])
        
        if not df_new.empty:
            # 3. Lógica de Consolidação (Arquivo Único)
            if os.path.exists(MASTER_FILE):
                df_old = pd.read_csv(MASTER_FILE)
                # Combina novo e velho, remove duplicados por timestamp e canal
                df_final = pd.concat([df_old, df_new]).drop_duplicates()
            else:
                df_final = df_new
            
            # 4. Salva o arquivo mestre
            df_final.to_csv(MASTER_FILE, index=False, encoding="utf-8-sig")
            print(f"Sucesso! {len(df_new)} novas linhas adicionadas ao {MASTER_FILE}")
        else:
            print("Nenhum dado novo encontrado nesta hora.")
    else:
        print("Falha na comunicação com a API da LI-COR.")
