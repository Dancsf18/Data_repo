# -*- coding: utf-8 -*-
"""
@author: Daniel Salgado
"""

import pandas as pd
from datetime import datetime
import sys
import unicodedata
import nltk
from nltk.corpus import stopwords
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

#%% Stopwords
nltk.download('stopwords')
sw = set(stopwords.words('spanish'))

def remove_stopwords(text: str) -> str:
    return " ".join([w for w in str(text).split() if w not in sw])

def normalize_ascii(text: str) -> str:
    return unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("utf-8")

#%% Rutas para clases auxiliares
sys.path.extend([
    r'C:/Users/User/Documents/Git/Data_repo/01_data_engineering/00. Clases'
])

from conexion_Mysql import conexion_DB
from Impresion import nice as ni

#%% Inicio
start_datetime = datetime.now()
print('start: ' + str(start_datetime))

#%% Conexión a la base de datos MySQL
connector = conexion_DB()
db = connector.conectar()

#%% Lectura Excel (dataset ejemplo)
df_ventas = pd.read_excel(
    r'C:/Users/User/Documents/Git/Data_repo/01_data_engineering/01. ETL_csv_mysql/Ventas.xlsx'
)

#%% Limpieza de títulos
df_ventas.columns = [
    normalize_ascii(
        remove_stopwords(col).strip().lower()
        .replace('/', '_').replace('.', '').replace(' ', '_')
    )
    for col in df_ventas.columns
]

#%% Carga a tabla auxiliar
table_name = "tb_aux_ventas"
try:
    df_ventas.to_sql(name=table_name, con=db, if_exists='append', index=False)
    ni.print_success(
        f'Datos cargados correctamente en la tabla {table_name}. Registros: {df_ventas.shape[0]}'
    )
except SQLAlchemyError as e:
    ni.print_error(f'Error al subir los datos a la base de datos: {e}')
except Exception as e:
    ni.print_error(f'Error inesperado al subir los datos a la base de datos: {e}')

#%% Ejecución de Stored Procedure
try:
    query = "CALL bbdd_pruebas.sp_001_ventas();"
    db.execute(text(query))
    db.connection.commit()
    ni.print_success("Stored procedure: sp_001_ventas ejecutado correctamente.")
except SQLAlchemyError as e:
    ni.print_error(f"Error al ejecutar el stored procedure: {e}")
except Exception as e:
    ni.print_error(f'Error inesperado al ejecutar el stored procedure: {e}')

#%% Cierre
connector.desconectar()

#%% Final
end_datetime = datetime.now()
print('executed: ' + str(end_datetime))
mins = int((end_datetime - start_datetime).total_seconds() / 60)
print('Tiempo de ejecución: ' + str(mins) + ' minutos')
