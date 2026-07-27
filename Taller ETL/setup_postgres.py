"""
Script de configuración inicial para PostgreSQL (pgAdmin).
Crea la base de datos y la tabla destino del taller ETL.
"""

import os

try:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    raise SystemExit("Instala psycopg2: pip install psycopg2-binary")

from config_db import POSTGRES_CONFIG


def _conectar(database=None):
    cfg = POSTGRES_CONFIG.copy()
    if database is not None:
        cfg["database"] = database
    return psycopg2.connect(**cfg)


def crear_base_datos():
    db_name = POSTGRES_CONFIG["database"]
    conn = _conectar("postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (db_name,),
    )
    if cursor.fetchone() is None:
        cursor.execute(f'CREATE DATABASE "{db_name}"')
        print(f"[OK] Base de datos '{db_name}' creada.")
    else:
        print(f"[OK] Base de datos '{db_name}' ya existe.")
    conn.close()


def crear_tabla():
    ruta_schema = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")
    with open(ruta_schema, "r", encoding="utf-8") as f:
        ddl = f.read()

    conn = _conectar()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(ddl)
    conn.close()
    print("[OK] Tabla admisiones_emergencia creada (schema.sql).")


if __name__ == "__main__":
    print("=" * 60)
    print(" SETUP POSTGRESQL - TALLER ETL CLÍNICA (pgAdmin)")
    print("=" * 60)
    crear_base_datos()
    crear_tabla()
    print("\nConéctate en pgAdmin a:")
    print(f"  Host:     {POSTGRES_CONFIG['host']}")
    print(f"  Puerto:   {POSTGRES_CONFIG['port']}")
    print(f"  Base de datos: {POSTGRES_CONFIG['database']}")
    print("  Tabla: admisiones_emergencia")
    print("\nConsultas de verificación:")
    print("  SELECT COUNT(*) FROM admisiones_emergencia;")
    print("  SELECT * FROM admisiones_emergencia ORDER BY fecha_ingreso DESC LIMIT 100;")
