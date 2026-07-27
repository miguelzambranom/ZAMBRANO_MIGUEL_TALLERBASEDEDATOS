"""
Script de configuración inicial para SQL Server (SSMS).
Crea la base de datos y la tabla destino del taller ETL.
"""

import os

try:
    import pyodbc
except ImportError:
    raise SystemExit("Instala pyodbc: pip install pyodbc")

from config_db import SQLSERVER_CONFIG


def _cadena_conexion(database="master"):
    cfg = SQLSERVER_CONFIG
    partes = [
        f"DRIVER={{{cfg['driver']}}}",
        f"SERVER={cfg['server']}",
        f"DATABASE={database}",
    ]
    if cfg.get("trusted_connection"):
        partes.append("Trusted_Connection=yes")
    else:
        partes.append(f"UID={cfg['uid']}")
        partes.append(f"PWD={cfg['pwd']}")
    if cfg.get("trust_server_certificate"):
        partes.append("TrustServerCertificate=yes")
    return ";".join(partes) + ";"


def crear_base_datos():
    db_name = SQLSERVER_CONFIG["database"]
    conn = pyodbc.connect(_cadena_conexion("master"))
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(
        f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE [{db_name}]"
    )
    conn.close()
    print(f"[OK] Base de datos '{db_name}' lista.")


def crear_tabla():
    db_name = SQLSERVER_CONFIG["database"]
    conn = pyodbc.connect(_cadena_conexion(db_name))
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("""
        IF OBJECT_ID('dbo.admisiones_emergencia', 'U') IS NOT NULL
            DROP TABLE dbo.admisiones_emergencia;
    """)

    cursor.execute("""
        CREATE TABLE dbo.admisiones_emergencia (
            id_admision       VARCHAR(36)      NOT NULL PRIMARY KEY,
            fecha_ingreso     DATETIME2        NOT NULL,
            id_paciente       VARCHAR(10)      NOT NULL,
            cama_asignada     VARCHAR(30)      NOT NULL,
            diagnostico       VARCHAR(100)     NOT NULL,
            costo_consulta    DECIMAL(10, 2)   NOT NULL,
            comision_seguro   DECIMAL(10, 2)   NOT NULL,
            costo_neto        DECIMAL(10, 2)   NOT NULL,
            alerta_gravedad   BIT              NOT NULL DEFAULT 0,
            estado_paciente   VARCHAR(20)      NOT NULL,
            fecha_carga       DATETIME2        NOT NULL DEFAULT SYSDATETIME()
        );
    """)

    cursor.execute("""
        CREATE INDEX idx_admisiones_fecha
        ON dbo.admisiones_emergencia (fecha_ingreso);
    """)
    cursor.execute("""
        CREATE INDEX idx_admisiones_paciente
        ON dbo.admisiones_emergencia (id_paciente);
    """)
    cursor.execute("""
        CREATE INDEX idx_admisiones_diagnostico
        ON dbo.admisiones_emergencia (diagnostico);
    """)

    conn.close()
    print("[OK] Tabla dbo.admisiones_emergencia creada.")


if __name__ == "__main__":
    print("=" * 60)
    print(" SETUP SQL SERVER - TALLER ETL CLÍNICA")
    print("=" * 60)
    crear_base_datos()
    crear_tabla()
    print("\nConéctate en SSMS a:")
    print(f"  Servidor: {SQLSERVER_CONFIG['server']}")
    print(f"  Base de datos: {SQLSERVER_CONFIG['database']}")
    print("  Tabla: dbo.admisiones_emergencia")
