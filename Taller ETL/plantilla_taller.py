"""
===============================================================================
PLANTILLA BASE PARA EL ESTUDIANTE: TALLER DE ETL CON GENERADORES (YIELD) - CLÍNICA
===============================================================================
Materia: Gestión de Datos / ETL Avanzado
Instrucciones: Completa los bloques señalados con '# TODO:' para implementar un
               pipeline de ETL capaz de procesar archivos masivos de admisiones
               médicas mediante streaming (generadores en Python con yield) sin
               agotar la memoria RAM de los servidores de la Clínica San José.
===============================================================================
"""

import csv
import os
import time
import tracemalloc
import sqlite3

from config_db import MOTOR_DB, POSTGRES_CONFIG, SQLSERVER_CONFIG, SQLITE_RUTA

# Intentar importar psycopg2 para PostgreSQL. Si el estudiante no lo tiene instalado o no tiene Postgres corriendo,
# se incluye soporte alternativo automático con SQLite para pruebas locales.
try:
    import psycopg2
    from psycopg2.extras import execute_values
    POSTGRES_DISPONIBLE = True
except ImportError:
    POSTGRES_DISPONIBLE = False

try:
    import pyodbc
    SQLSERVER_DISPONIBLE = True
except ImportError:
    SQLSERVER_DISPONIBLE = False


# =============================================================================
# FASE 1: EXTRACT - GENERADOR CON YIELD (STREAMING / LAZY EVALUATION)
# =============================================================================
def extractor_lotes_csv(ruta_csv, tamano_lote=2000):
    """
    Función Generadora que lee un archivo CSV gigante de admisiones de forma perezosa (Lazy)
    usando la instrucción 'yield'. En lugar de cargar todo el archivo en RAM,
    retorna un lote (chunk) de registros a la vez.

    Parámetros:
        ruta_csv (str): Ruta al archivo CSV.
        tamano_lote (int): Cantidad de filas por cada lote producido.

    Yields:
        list[dict]: Lista de diccionarios representando las filas de un lote.
    """
    if not os.path.exists(ruta_csv):
        raise FileNotFoundError(f"No existe el archivo: {ruta_csv}")

    # Lectura streaming: una fila a la vez, sin cargar el CSV completo en RAM
    with open(ruta_csv, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        lote = []
        for fila in reader:
            lote.append(fila)
            # Emitir lote cuando alcanza el tamaño configurado (evaluación perezosa)
            if len(lote) >= tamano_lote:
                yield lote
                lote = []
        # Emitir registros remanentes del último lote incompleto
        if lote:
            yield lote


# =============================================================================
# FASE 2: TRANSFORM - REGLAS DE NEGOCIO Y LIMPIEZA LOTE A LOTE (CLÍNICA)
# =============================================================================
def transformar_lote(lote_raw):
    """
    Transforma un lote de registros crudos aplicando reglas de limpieza
    y cálculos clínicos de la clínica San José.

    Reglas de Negocio:
    1. Descartar registros con costo_consulta vacío o menor/igual a 0 (Limpieza).
    2. Calcular Comisión de Seguro del 5% sobre el costo_consulta (procesamiento administrativo).
    3. Calcular Costo Neto = costo_consulta - comision_seguro.
    4. Marcar bandera 'alerta_gravedad' = True si el costo > 200.00 y el estado del paciente es 'Critico' o 'Grave'.

    Returns:
        list[tuple]: Lista de tuplas estructuradas para inserción SQL en lote.
    """
    lote_transformado = []

    for reg in lote_raw:
        try:
            # --- Limpieza: descartar costos nulos, vacíos o inválidos ---
            costo_str = reg.get("costo_consulta", "")
            if costo_str is None or str(costo_str).strip() == "":
                continue

            costo = float(costo_str)
            if costo <= 0:
                continue

            # --- Cálculos administrativos de la clínica ---
            comision_seguro = round(costo * 0.05, 2)
            costo_neto = round(costo - comision_seguro, 2)

            # --- Regla de alerta de gravedad ---
            estado = reg.get("estado_paciente", "Leve")
            alerta_gravedad = (costo > 200.0) and (estado in ("Grave", "Critico"))

            tupla_registro = (
                reg["id_admision"],
                reg["fecha_ingreso"],
                reg["id_paciente"],
                reg["cama_asignada"],
                reg["diagnostico"],
                costo,
                comision_seguro,
                costo_neto,
                alerta_gravedad,
                estado,
            )
            lote_transformado.append(tupla_registro)

        except (ValueError, TypeError, KeyError):
            # Omitir filas corruptas o con columnas faltantes
            continue

    return lote_transformado


# =============================================================================
# FASE 3: LOAD - CARGA EN LOTE (BATCH LOAD) EN BASE DE DATOS
# =============================================================================
def cargar_lote_sqlite(conn, lote_transformado):
    """
    Carga un lote de registros transformados en la base de datos SQLite.
    """
    if not lote_transformado:
        return

    sql = """
    INSERT INTO admisiones_emergencia 
    (id_admision, fecha_ingreso, id_paciente, cama_asignada, diagnostico, 
     costo_consulta, comision_seguro, costo_neto, alerta_gravedad, estado_paciente)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    cursor = conn.cursor()
    cursor.executemany(sql, lote_transformado)
    conn.commit()


def cargar_lote_postgres(conn, lote_transformado):
    """
    Carga un lote de registros transformados en PostgreSQL utilizando execute_values.
    """
    if not lote_transformado:
        return

    sql = """
    INSERT INTO admisiones_emergencia 
    (id_admision, fecha_ingreso, id_paciente, cama_asignada, diagnostico, 
     costo_consulta, comision_seguro, costo_neto, alerta_gravedad, estado_paciente)
    VALUES %s;
    """
    cursor = conn.cursor()
    execute_values(cursor, sql, lote_transformado)
    conn.commit()


def cargar_lote_sqlserver(conn, lote_transformado):
    """
    Carga un lote de registros transformados en SQL Server (SSMS) con executemany.
    """
    if not lote_transformado:
        return

    sql = """
    INSERT INTO dbo.admisiones_emergencia
    (id_admision, fecha_ingreso, id_paciente, cama_asignada, diagnostico,
     costo_consulta, comision_seguro, costo_neto, alerta_gravedad, estado_paciente)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    cursor = conn.cursor()
    cursor.fast_executemany = True
    cursor.executemany(sql, lote_transformado)
    conn.commit()


def conectar_postgres():
    """Abre conexión a PostgreSQL usando la configuración de config_db.py."""
    return psycopg2.connect(**POSTGRES_CONFIG)


def conectar_sqlserver():
    """Abre conexión a SQL Server usando la configuración de config_db.py."""
    cfg = SQLSERVER_CONFIG
    partes = [
        f"DRIVER={{{cfg['driver']}}}",
        f"SERVER={cfg['server']}",
        f"DATABASE={cfg['database']}",
    ]
    if cfg.get("trusted_connection"):
        partes.append("Trusted_Connection=yes")
    else:
        partes.append(f"UID={cfg['uid']}")
        partes.append(f"PWD={cfg['pwd']}")
    if cfg.get("trust_server_certificate"):
        partes.append("TrustServerCertificate=yes")

    return pyodbc.connect(";".join(partes) + ";")


def cargar_lote_db(conn, lote_transformado, motor=MOTOR_DB):
    """
    Punto de entrada unificado para la carga por lotes.
    Selecciona SQLite, PostgreSQL o SQL Server según el motor configurado.
    """
    if motor == "sqlserver":
        cargar_lote_sqlserver(conn, lote_transformado)
    elif motor == "postgres" and POSTGRES_DISPONIBLE:
        cargar_lote_postgres(conn, lote_transformado)
    else:
        cargar_lote_sqlite(conn, lote_transformado)


# =============================================================================
# EJECUCIÓN PRINCIPAL Y MONITOREO DE MEMORIA RAM
# =============================================================================
def ejecutar_pipeline(motor=MOTOR_DB):
    directorio_base = os.path.dirname(os.path.abspath(__file__))
    ruta_csv = os.path.join(directorio_base, "logs_admisiones_masivas.csv")
    ruta_db_sqlite = os.path.join(directorio_base, SQLITE_RUTA)

    print("=" * 70)
    print(" INICIANDO PIPELINE ETL CON GENERADORES - CLÍNICA SAN JOSÉ ")
    print("=" * 70)
    print(f" -> Motor de destino: {motor.upper()}")

    # Medición de consumo de memoria RAM (tracemalloc)
    tracemalloc.start()
    tiempo_inicio = time.time()

    if motor == "postgres":
        if not POSTGRES_DISPONIBLE:
            raise ImportError("Instala psycopg2: pip install psycopg2-binary")
        conn = conectar_postgres()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM admisiones_emergencia;")
        conn.commit()
        print(
            f" -> Conectado a PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']} "
            f"/ {POSTGRES_CONFIG['database']}"
        )
    elif motor == "sqlserver":
        if not SQLSERVER_DISPONIBLE:
            raise ImportError("Instala pyodbc para usar SQL Server: pip install pyodbc")
        conn = conectar_sqlserver()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dbo.admisiones_emergencia;")
        conn.commit()
        print(f" -> Conectado a SQL Server: {SQLSERVER_CONFIG['server']} / {SQLSERVER_CONFIG['database']}")
    else:
        # Fallback local en SQLite
        conn = sqlite3.connect(ruta_db_sqlite)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admisiones_emergencia (
                id_admision TEXT PRIMARY KEY,
                fecha_ingreso TEXT,
                id_paciente TEXT,
                cama_asignada TEXT,
                diagnostico TEXT,
                costo_consulta REAL,
                comision_seguro REAL,
                costo_neto REAL,
                alerta_gravedad INTEGER,
                estado_paciente TEXT
            );
        """)
        cursor.execute("DELETE FROM admisiones_emergencia;")
        conn.commit()
        print(f" -> Usando SQLite local: {ruta_db_sqlite}")

    total_procesados = 0
    total_lotes = 0

    print("-> Procesando archivo masivo en lotes (Streaming via yield)...")

    # Bucle del ETL: iteramos directamente sobre el GENERADOR (lazy evaluation)
    for lote_raw in extractor_lotes_csv(ruta_csv, tamano_lote=2000):
        total_lotes += 1

        # 1. Transformar lote
        lote_listo = transformar_lote(lote_raw)

        # 2. Cargar lote en base de datos (batch load con commit por lote)
        cargar_lote_db(conn, lote_listo, motor=motor)

        total_procesados += len(lote_listo)

        # Reportar estado y pico de RAM en tiempo real
        peak_ram_mb = tracemalloc.get_traced_memory()[1] / (1024 * 1024)
        print(
            f"   [Lote #{total_lotes:02d}] Cargadas {len(lote_listo):,} filas | "
            f"Acumulado: {total_procesados:,} | RAM Pico: {peak_ram_mb:.2f} MB"
        )

    conn.close()
    duracion = time.time() - tiempo_inicio
    memoria_final_mb = tracemalloc.get_traced_memory()[1] / (1024 * 1024)
    tracemalloc.stop()

    print("\n-------------------------------------------------------------------")
    print(" SUMMARY DE RENDIMIENTO DEL PIPELINE:")
    print("-------------------------------------------------------------------")
    print(f" - Filas totales cargadas con éxito: {total_procesados:,}")
    print(f" - Lotes procesados:                 {total_lotes}")
    print(f" - Tiempo de ejecución:              {duracion:.2f} segundos")
    print(f" - Consumo máximo de RAM (Pico RAM): {memoria_final_mb:.2f} MB")
    print("===================================================================")
    print(" [ÉXITO] El consumo de RAM se mantuvo CONSTANTE gracias al uso de yield.")
    if motor == "postgres":
        print("\n Abre pgAdmin y ejecuta en la base de datos del taller:")
        print(f"   -- Base de datos: {POSTGRES_CONFIG['database']}")
        print("   SELECT COUNT(*) AS total FROM admisiones_emergencia;")
        print("   SELECT * FROM admisiones_emergencia ORDER BY fecha_ingreso DESC LIMIT 100;")
        print("   SELECT COUNT(*) AS alertas FROM admisiones_emergencia WHERE alerta_gravedad = TRUE;")
    elif motor == "sqlserver":
        print("\n Abre SQL Server Management Studio (SSMS) y ejecuta:")
        print(f"   USE {SQLSERVER_CONFIG['database']};")
        print("   SELECT COUNT(*) AS total FROM dbo.admisiones_emergencia;")
        print("   SELECT TOP 100 * FROM dbo.admisiones_emergencia ORDER BY fecha_ingreso DESC;")


if __name__ == "__main__":
    ejecutar_pipeline()