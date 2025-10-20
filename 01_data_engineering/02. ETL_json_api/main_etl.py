# -*- coding: utf-8 -*-
"""
@author: Daniel
"""
#%% Imports
from datetime import datetime, timezone
import sys
import requests
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

#%% Rutas a clases auxiliares
sys.path.extend([
    r'C:/Users/User/Documents/Git/Data_repo/01_data_engineering/00. Clases'
])

from conexion_Mysql import conexion_DB
from Impresion import nice as ni

#%% Parámetros
API_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false"
)
TABLE_AUX = "tb_aux_crypto_markets"     # staging
SP_NAME   = "sp_upsert_crypto_markets"  # procedure post-carga

#%% Inicio
start_datetime = datetime.now()
ni.print_header("ETL JSON -> MySQL (CoinGecko)")

#%% Conexión a la base de datos
connector = conexion_DB()
db = connector.conectar()

#%% EXTRACT
try:
    ni.print_section("EXTRACT")
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    df_raw = pd.json_normalize(data)
    ni.print_success(f"JSON recibido. Registros: {len(df_raw)}")
except Exception as e:
    ni.print_error(f"Fallo en extracción: {e}")
    connector.desconectar()
    raise

#%% TRANSFORM
try:
    ni.print_section("TRANSFORM")
    cols = [
        "id","symbol","name","market_cap_rank","current_price","market_cap",
        "total_volume","price_change_percentage_24h","circulating_supply",
        "ath","ath_change_percentage","atl","atl_change_percentage","last_updated"
    ]
    keep = [c for c in cols if c in df_raw.columns]
    df = df_raw[keep].copy()
    # Tipos numéricos seguros
    num_cols = [
        "market_cap_rank","current_price","market_cap","total_volume",
        "price_change_percentage_24h","circulating_supply","ath",
        "ath_change_percentage","atl","atl_change_percentage"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Sello de ingestión
    df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
    ni.print_success(f"Transform OK. Columnas finales: {list(df.columns)}")
except Exception as e:
    ni.print_error(f"Fallo en transformación: {e}")
    connector.desconectar()
    raise

#%% LOAD -> tabla auxiliar
try:
    ni.print_section("LOAD")
    df.to_sql(TABLE_AUX, con=db, if_exists="append", index=False)
    ni.print_success(f"Cargados {len(df)} registros en {TABLE_AUX}.")
except SQLAlchemyError as e:
    ni.print_error(f"Error al cargar en {TABLE_AUX}: {e}")
    connector.desconectar()
    raise
except Exception as e:
    ni.print_error(f"Error inesperado en carga: {e}")
    connector.desconectar()
    raise

#%% POST-LOAD -> Stored Procedure
try:
    ni.print_section("POST-LOAD")
    with db.begin() as conn:
       db.execute(text(f"CALL {SP_NAME}();"))
    ni.print_success(f"Stored procedure {SP_NAME} ejecutado correctamente.")
except SQLAlchemyError as e:
    ni.print_error(f"Error al ejecutar {SP_NAME}: {e}")
    connector.desconectar()
    raise
except Exception as e:
    ni.print_error(f"Error inesperado al ejecutar {SP_NAME}: {e}")
    connector.desconectar()
    raise

#%% Cierre
connector.desconectar()

#%% Final
end_datetime = datetime.now()
ni.print_footer(f"Fin | Duración: {((end_datetime - start_datetime).total_seconds()/60):.2f} min")
