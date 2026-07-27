"""

Configuración de conexión para el taller ETL.

Ajusta estos valores según tu instalación local (pgAdmin / PostgreSQL).

"""



# Motor de destino: "postgres" | "sqlite" | "sqlserver"

MOTOR_DB = "postgres"



# --- PostgreSQL (pgAdmin) ---

POSTGRES_CONFIG = {

    "host": "localhost",

    "port": 5433,

    "database": "taller_etl_clinica",

    "user": "postgres",

    "password": "Mcm221277",

}



# --- SQL Server (SQL Server Management Studio) ---

SQLSERVER_CONFIG = {

    "driver": "ODBC Driver 17 for SQL Server",

    "server": "localhost",

    "database": "TallerETL_Clinica",

    "trusted_connection": True,

    "uid": "",

    "pwd": "",

    "trust_server_certificate": True,

}



# --- SQLite (fallback local) ---

SQLITE_RUTA = "taller_etl_resultado.db"


