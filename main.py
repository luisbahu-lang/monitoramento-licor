import os
import json
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests

# ============================================
# CONFIGURAÇÕES AUTOMATIZADAS
# ============================================
BASE_URL = "https://api.licor.cloud"
SOURCE_LINK = "https://www.licor.cloud/devices/6c53cc52-fb78-4318-a2c6-81b47ddb02eb/connected-scene/quick-chart/22127972-1"
API_TOKEN = os.getenv("LICOR_TOKEN") # Puxa do segredo que você criou
STATION_HINT = "agropv"

# Define a janela: da última hora até agora
now = datetime.now(timezone.utc)
START_UTC = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
END_UTC   = now.strftime("%Y-%m-%d %H:%M:%S")

DATA_DIR = Path("dados_chuva")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_CSV = DATA_DIR / f"chuva_{now.strftime('%Y%m%d_%H')}.csv"

# ============================================
# SUAS FUNÇÕES ORIGINAIS (INTEGRADAS)
# ============================================

def extract_ids_from_link(link):
    device_uuid, tail_id = None, None
    m1 = re.search(r"/devices/([0-9a-fA-F-]{36})", link)
    if m1: device_uuid = m1.group(1)
    m2 = re.search(r"/quick-chart/([^/?#]+)", link)
    if m2: tail_id = m2.group(1)
    return device_uuid, tail_id

def api_get(path, params=None):
    url = f"{BASE_URL}{path}"
    r = session.get(url, params=params, timeout=60)
    try:
        payload = r.json() if "json" in r.headers.get("content-type", "").lower() else r.text
    except: payload = r.text
    if not r.ok: raise RuntimeError(f"Erro HTTP {r.status_code}")
    return payload

def parse_utc(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

def fmt_utc(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def daterange_chunks(start_utc, end_utc, chunk_hours=24):
    start, end = parse_utc(start_utc), parse_utc(end_utc)
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(hours=chunk_hours) - timedelta(seconds=1), end)
        yield fmt_utc(cur), fmt_utc(nxt)
        cur = nxt + timedelta(seconds=1)

def flatten_nodes(obj, path="root"):
    out = []
    if isinstance(obj, dict):
        out.append((path, obj))
        for k, v in obj.items(): out.extend(flatten_nodes(v, f"{path}.{k}"))
    elif isinstance(obj, list):
        for i, v in enumerate(obj): out.extend(flatten_nodes(v, f"{path}[{i}]"))
    return out

def get_first_present(node, keys):
    for k in keys:
        if k in node and node[k] is not None and str(node[k]).strip(): return str(node[k]).strip()
    return None

def get_devices_payload():
    for path in ["/v2/devices", "/v1/devices"]:
        try: return api_get(path)
        except: continue
    raise RuntimeError("Falha ao listar dispositivos")

def build_candidate_table(devices_payload, device_uuid=None, link_tail_id=None, station_hint=None):
    rows = []
    for path, node in flatten_nodes(devices_payload):
        if not isinstance(node, dict): continue
        blob = json.dumps(node).lower()
        row = {
            "match_uuid": bool(device_uuid and device_uuid.lower() in blob),
            "match_tail": bool(link_tail_id and link_tail_id.lower() in blob),
            "match_hint": bool(station_hint and station_hint.lower() in blob),
            "deviceSerialNumber": get_first_present(node, ["deviceSerialNumber", "serial"]),
            "logger_id": get_first_present(node, ["logger_id", "loggerId"]),
            "id": get_first_present(node, ["id", "deviceId"])
        }
        if any([row["match_uuid"], row["match_tail"], row["match_hint"]]): rows.append(row)
    return pd.DataFrame(rows).drop_duplicates()

def extract_candidate_ids(df):
    ids = []
    for col in ["deviceSerialNumber", "logger_id", "id"]:
        if col in df.columns: ids.extend(df[col].dropna().unique().tolist())
    return list(dict.fromkeys(ids))

def fetch_data_once(identifier, start, end):
    for path in ["/v1/data", "/v2/data"]:
        for key in ["loggers", "logger", "devices", "device"]:
            try:
                payload = api_get(path, {key: identifier, "start_date_time": start, "end_date_time": end})
                rows = payload.get("data", []) if isinstance(payload, dict) else []
                if rows: return rows
            except: continue
    return []

def select_rain_data(df):
    if df.empty: return df
    patterns = ["rain", "precip", "chuva", "pluv"]
    mask = df.astype(str).apply(lambda x: x.str.contains("|".join(patterns), case=False)).any(axis=1)
    return df[mask] if mask.any() else pd.DataFrame()

# ============================================
# EXECUÇÃO PRINCIPAL
# ============================================
if __name__ == "__main__":
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {API_TOKEN}", "Accept": "application/json"})
    
    try:
        dev_payload = get_devices_payload()
        uuid_l, tail_l = extract_ids_from_link(SOURCE_LINK)
        df_c = build_candidate_table(dev_payload, uuid_l, tail_l, STATION_HINT)
        c_ids = extract_candidate_ids(df_c)
        
        all_rows = []
        for cid in c_ids:
            rows = fetch_data_once(cid, START_UTC, END_UTC)
            if rows: 
                all_rows = rows
                break
        
        df = pd.json_normalize(all_rows)
        df_rain = select_rain_data(df)
        
        if not df_rain.empty:
            df_rain.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
            print(f"Arquivo salvo: {OUTPUT_CSV}")
    except Exception as e:
        print(f"Erro: {e}")
