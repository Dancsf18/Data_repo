# -*- coding: utf-8 -*-
"""
@author: Daniel
"""

#%% Imports
from datetime import datetime, timezone
import sys
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import feedparser

#%% Rutas a clases auxiliares
sys.path.extend([
    r'C:/Users/User/Documents/Git/Data_repo/01_data_engineering/00. Clases'
])

from conexion_Mysql import conexion_DB
from Impresion import nice as ni

#%% Parámetros
FEED_URL  = "https://feeds.bbci.co.uk/news/world/rss.xml"  # puedes cambiar por otro feed RSS
TABLE_AUX = "tb_aux_news_rss"        # staging
SP_NAME   = "sp_upsert_news_rss"     # procedure post-carga
MAX_ITEMS = 100                      # límite de items a procesar

#%% Inicio
start_datetime = datetime.now()
ni.print_header("ETL XML/RSS -> MySQL (News Feed)")

#%% Conexión a la base de datos
connector = conexion_DB()
db = connector.conectar()

#%% EXTRACT
try:
    ni.print_section("EXTRACT")
    feed = feedparser.parse(FEED_URL)
    if feed.bozo:
        raise ValueError(f"Error al leer el feed RSS: {getattr(feed, 'bozo_exception', 'desconocido')}")
    entries = feed.entries[:MAX_ITEMS] if MAX_ITEMS else feed.entries

    rows = []
    for e in entries:
        rows.append({
            "guid": getattr(e, "id", None) or getattr(e, "guid", None),
            "title": getattr(e, "title", None),
            "link": getattr(e, "link", None),
            "published": getattr(e, "published", None) or getattr(e, "updated", None),
            "summary": getattr(e, "summary", None),
            "source_title": getattr(feed.feed, "title", None),
            "source_url": FEED_URL
        })

    df_raw = pd.DataFrame(rows)
    if df_raw.empty:
        raise ValueError("El feed no devolvió items para procesar.")
    ni.print_success(f"RSS leído. Registros extraídos: {len(df_raw)}")
except Exception as e:
    ni.print_error(f"Fallo en extracción: {e}")
    connector.desconectar()
    raise

#%% TRANSFORM
try:
    ni.print_section("TRANSFORM")
    df = df_raw.copy()

    # Normalización básica
    for col in ["guid", "title", "link", "summary", "source_title", "source_url"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    if "published" in df.columns:
        df["published_ts"] = pd.to_datetime(df["published"], errors="coerce", utc=True)
    else:
        df["published_ts"] = pd.NaT

    # Sello de ingestión
    df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()

    # Dedupe por guid o por (title+link)
    if "guid" in df.columns and df["guid"].notna().any():
        df.drop_duplicates(subset=["guid"], inplace=True)
    else:
        df.drop_duplicates(subset=["title", "link"], inplace=True)

    ni.print_success(f"Transform OK. Registros finales: {len(df)} | Columnas: {list(df.columns)}")
except Exception as e:
    ni.print_error(f"Fallo en transformación: {e}")
    connector.desconectar()
    raise

#%% LOAD -> tabla auxiliar
try:
    ni.print_section("LOAD")
    with db.begin() as conn:
        df.to_sql(TABLE_AUX, con=conn, if_exists="append", index=False)
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
        conn.execute(text(f"CALL {SP_NAME}();"))
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
