# -*- coding: utf-8 -*-
"""
Autor: Daniel
"""

#%% Imports
from datetime import datetime, timezone
import sys
import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import spacy

#%% Rutas a clases auxiliares
sys.path.extend([
    r'C:/Users/User/Documents/Git/Data_repo/01_data_engineering/00. Clases'
])

from conexion_Mysql import conexion_DB
from Impresion import nice as ni

#%% Parámetros
INPUT_FILE = r"C:/Users/User/Documents/Git/Data_repo/01_data_engineering/03_etl_spacy_text/reviews.csv"  # CSV con columnas: id, text
TEXT_COL   = "text"
ID_COL     = "id"

TABLE_AUX  = "tb_aux_reviews_nlp"      # staging
SP_NAME    = "sp_upsert_reviews_nlp"   # procedure post-carga

SPACY_MODEL = "es_core_news_md"        # si no tienes este, usa "es_core_news_sm"
BATCH_SIZE  = 200
N_PROC      = os.cpu_count() or 2

#%% Inicio
start_datetime = datetime.now()
ni.print_header("ETL NLP (spaCy ES) -> MySQL")

#%% Cargar modelo spaCy
try:
    ni.print_section("LOAD NLP MODEL")
    nlp = spacy.load(SPACY_MODEL, disable=["textcat"])
    ni.print_success(f"Modelo spaCy cargado: {SPACY_MODEL}")
except Exception as e:
    ni.print_error(f"No se pudo cargar {SPACY_MODEL}. Instala el modelo o usa es_core_news_sm. Error: {e}")
    raise

#%% Conexión a la base de datos
connector = conexion_DB()
db = connector.conectar()

#%% EXTRACT
try:
    ni.print_section("EXTRACT")
    df_raw = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    if ID_COL not in df_raw.columns or TEXT_COL not in df_raw.columns:
        raise ValueError(f"El CSV debe contener columnas '{ID_COL}' y '{TEXT_COL}'.")
    df_raw[TEXT_COL] = df_raw[TEXT_COL].astype(str).fillna("")
    ni.print_success(f"CSV leído. Registros: {len(df_raw)}")
except Exception as e:
    ni.print_error(f"Fallo en extracción: {e}")
    connector.desconectar()
    raise

#%% TRANSFORM (tokenización, lemas, entidades)
try:
    ni.print_section("TRANSFORM")

    # Procesamos en streaming para eficiencia
    docs = nlp.pipe(df_raw[TEXT_COL].tolist(), batch_size=BATCH_SIZE, n_process=N_PROC)

    rows_tokens = []
    rows_entities = []

    for (doc_id, doc) in zip(df_raw[ID_COL].tolist(), docs):
        # Tabla de tokens (limpia, sin espacios)
        for tok in doc:
            if tok.is_space:
                continue
            rows_tokens.append({
                ID_COL: doc_id,
                "token": tok.text,
                "lemma": tok.lemma_,
                "pos": tok.pos_,
                "tag": tok.tag_,
                "dep": tok.dep_,
                "is_stop": bool(tok.is_stop),
            })
        # Tabla de entidades
        for ent in doc.ents:
            rows_entities.append({
                ID_COL: doc_id,
                "ent_text": ent.text,
                "ent_label": ent.label_,
                "start_char": ent.start_char,
                "end_char": ent.end_char
            })

    df_tokens = pd.DataFrame(rows_tokens)
    df_ents   = pd.DataFrame(rows_entities)

    # Marca de tiempo de ingestión
    ts = datetime.now(timezone.utc).isoformat()
    if not df_tokens.empty:
        df_tokens["ingestion_ts"] = ts
    if not df_ents.empty:
        df_ents["ingestion_ts"] = ts

    ni.print_success(
        f"Transform OK. Tokens: {len(df_tokens)} | Entidades: {len(df_ents)}"
    )

except Exception as e:
    ni.print_error(f"Fallo en transformación: {e}")
    connector.desconectar()
    raise

#%% LOAD -> tablas auxiliares (tokens y entidades)
try:
    ni.print_section("LOAD")
    with db.begin() as conn:
        if not df_tokens.empty:
            df_tokens.to_sql(f"{TABLE_AUX}_tokens", con=conn, if_exists="append", index=False)
        if not df_ents.empty:
            df_ents.to_sql(f"{TABLE_AUX}_entities", con=conn, if_exists="append", index=False)
    ni.print_success(
        f"Carga OK. {TABLE_AUX}_tokens: {len(df_tokens)} filas | {TABLE_AUX}_entities: {len(df_ents)} filas"
    )
except SQLAlchemyError as e:
    ni.print_error(f"Error al cargar staging: {e}")
    connector.desconectar()
    raise
except Exception as e:
    ni.print_error(f"Error inesperado en carga: {e}")
    connector.desconectar()
    raise

#%% POST-LOAD -> Stored Procedure (consolidación / upsert)
try:
    ni.print_section("POST-LOAD")
    with db.begin() as conn:
        # Ajusta el SP para que lea de *_tokens y *_entities y consolide a tablas finales
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
