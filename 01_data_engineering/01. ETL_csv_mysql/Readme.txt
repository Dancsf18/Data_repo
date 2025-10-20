# ETL CSV → MySQL con Stored Procedure

Pipeline simple de ingeniería de datos:
1) **Extract**: lectura de un archivo `Ventas.xlsx` (puede ser CSV/Excel).
2) **Transform**: limpieza mínima de encabezados (stopwords, normalización).
3) **Load**: carga a tabla auxiliar en MySQL.
4) **Post-load**: ejecución de `CALL licuavent.sp_001_licuavent_articulos();`.

## Estructura
