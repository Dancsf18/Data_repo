# -*- coding: utf-8 -*-
"""
@author: Daniel
"""

#%% Imports
from datetime import datetime, timezone
import sys
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

#%% Rutas a clases auxiliares
sys.path.extend([
    r'C:/Users/User/Documents/Git/Data_repo/01_data_engineering/00. Clases'
])

from conexion_Mysql import conexion_DB
from Impresion import nice as ni

#%% Parámetros
BASE_URL   = "https://quotes.toscrape.com/page/{}/"
PAGES      = 5  # número de páginas a scrapear
SLEEP_SEC  = 1  # pausa entre requests para ser amable con el sitio
TABLE_AUX  = "tb_aux_quotes"        # staging
SP_NAME    = "sp_upsert_quotes"     # procedure post-carga

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

#%% Inicio
start_datetime = datetime.now()
ni.print_header("ETL WEB SCRAPING -> MySQL (quotes.toscrape.com)")

#%% Conexión a la base de datos
connector = conexion_DB()
db = connector.conectar()

#%% EXTRACT
try:
    ni.print_section("EXTRACT")
    rows = []
    for p in range(1, PAGES + 1):
        url = BASE_URL.format(p)
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        quotes = soup.select("div.quote")
        if not quotes:
            ni.print_warning(f"Página {p} sin resultados, deteniendo paginación.")
            break

        for q in quotes:
            text_q   = q.select_one("span.text")
            author_q = q.select_one("small.author")
            tags_q   = [t.get_text(strip=True) for t in q.select("div.tags a.tag")]

            rows.append({
                "quote": text_q.get_text(strip=True) if text_q else None,
                "author": author_q.get_text(strip=True) if author_q else None,
                "tags": ",".join(tags_q) if tags_q else None,
                "source_url": url
            })
        time.sleep(SLEEP_SEC)

    df_raw = pd.DataFrame(rows)
    ni.print_success(f"Scraping OK. Registros extraídos: {len(df_raw)}")
except Exception as e:
    ni.print_error(f"Fallo en extracción: {e}")
    connector.desconectar()
    raise

#%% TRANSFORM
try:
    ni.print_section("TRANSFORM")
    if df_raw.empty:
        raise ValueError("No se extrajeron registros para transformar.")

    df = df_raw.copy()
    df["quote"]  = df["quote"].astype(str).str.strip()
    df["author"] = df["author"].astype(str).str.strip()
    df["tags"]   = df["tags"].astype(str).str.strip()
    df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()

    # Elimina duplicados por quote+author
    df.drop_duplicates(subset=["quote", "author"], inplace=True)

    ni.print_success(f"Transform OK. Registros finales: {len(df)}")
except Exception as e:
    ni.print_error(f"Fallo en transformación: {e}")
    connector.desconectar()
    raise

#%% LOAD -> tabla auxiliar
try:
    ni.print_section("LOAD")
    with db.begin() as conn:
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
