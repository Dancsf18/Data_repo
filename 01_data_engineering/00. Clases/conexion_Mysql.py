# -*- coding: utf-8 -*-
"""
@author: Daniel Salgado
"""

# -*- coding: utf-8 -*-
import sqlalchemy
from urllib.parse import quote_plus


class conexion_DB:
    def __init__(
        self,
        host: str = "localhost",          # ← ahora apunta a tu servidor
        user: str = "root",
        password: str = "Licuavent123",
        database: str = "bbdd_pruebas",
        port: int = 3306,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self._engine = None
        self._conn = None

    def conectar(self):
        if self._conn is None:
            url = (
                f"mysql+pymysql://{self.user}:{quote_plus(self.password)}"
                f"@{self.host}:{self.port}/{self.database}"
            )
            self._engine = sqlalchemy.create_engine(url, pool_pre_ping=True)
            self._conn = self._engine.connect()
        return self._conn

    def desconectar(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
